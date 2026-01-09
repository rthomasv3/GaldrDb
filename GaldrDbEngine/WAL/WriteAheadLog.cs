using System;
using System.Collections.Generic;
using System.IO;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.WAL;

public class WriteAheadLog : IDisposable
{
    private readonly string _walPath;
    private readonly int _pageSize;
    private readonly object _writeLock;
    private FileStream _walStream;
    private WalHeader _header;
    private long _currentFrameNumber;
    private bool _disposed;

    public WriteAheadLog(string walPath, int pageSize)
    {
        _walPath = walPath;
        _pageSize = pageSize;
        _writeLock = new object();
        _disposed = false;
    }

    public WalHeader Header => _header;
    public long CurrentFrameNumber => _currentFrameNumber;
    public string WalPath => _walPath;

    public void Create()
    {
        if (File.Exists(_walPath))
        {
            throw new InvalidOperationException($"WAL file already exists: {_walPath}");
        }

        _walStream = new FileStream(_walPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);

        _header = new WalHeader
        {
            PageSize = _pageSize,
            CheckpointTxId = 0,
            FrameCount = 0
        };

        WriteHeader();
        _currentFrameNumber = 0;
    }

    public void Open()
    {
        if (!File.Exists(_walPath))
        {
            throw new FileNotFoundException($"WAL file not found: {_walPath}");
        }

        _walStream = new FileStream(_walPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);

        // Handle empty WAL file (can occur after truncation or crash during creation)
        if (_walStream.Length == 0)
        {
            _header = new WalHeader();
            _header.PageSize = _pageSize;
            byte[] headerBytes = _header.Serialize();
            _walStream.Write(headerBytes, 0, headerBytes.Length);
            _walStream.Flush();
            _currentFrameNumber = 0;
        }
        else
        {
            byte[] headerBuffer = new byte[WalHeader.HEADER_SIZE];
            _walStream.Position = 0;
            int bytesRead = _walStream.Read(headerBuffer, 0, WalHeader.HEADER_SIZE);

            if (bytesRead < WalHeader.HEADER_SIZE)
            {
                throw new InvalidOperationException("WAL file is corrupted: header too short");
            }

            _header = WalHeader.Deserialize(headerBuffer);

            if (!_header.ValidateMagicNumber())
            {
                throw new InvalidOperationException("WAL file is corrupted: invalid magic number");
            }

            if (_header.PageSize != _pageSize)
            {
                throw new InvalidOperationException($"WAL page size mismatch: expected {_pageSize}, found {_header.PageSize}");
            }

            _currentFrameNumber = _header.FrameCount;
        }
    }

    public long WriteFrame(ulong txId, int pageId, byte pageType, byte[] data, WalFrameFlags flags)
    {
        lock (_writeLock)
        {
            _currentFrameNumber++;

            byte[] frameData = data ?? Array.Empty<byte>();

            WalFrame frame = new WalFrame
            {
                FrameNumber = _currentFrameNumber,
                TxId = txId,
                PageId = pageId,
                PageType = pageType,
                Flags = flags,
                Data = frameData
            };

            int frameBufferSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
            byte[] frameBuffer = BufferPool.Rent(frameBufferSize);
            try
            {
                int bytesWritten = frame.SerializeTo(frameBuffer);

                long framePosition = WalHeader.HEADER_SIZE + (_currentFrameNumber - 1) * (WalFrame.FRAME_HEADER_SIZE + _pageSize);
                _walStream.Position = framePosition;
                _walStream.Write(frameBuffer, 0, bytesWritten);

                // Pad to full page size if data is smaller
                int paddingNeeded = _pageSize - frameData.Length;
                if (paddingNeeded > 0)
                {
                    // Clear the padding area in the same buffer and write it
                    Array.Clear(frameBuffer, 0, paddingNeeded);
                    _walStream.Write(frameBuffer, 0, paddingNeeded);
                }
            }
            finally
            {
                BufferPool.Return(frameBuffer);
            }

            _header.FrameCount = _currentFrameNumber;
            WriteHeader();

            return _currentFrameNumber;
        }
    }

    public List<WalFrame> ReadAllFrames()
    {
        List<WalFrame> frames = new List<WalFrame>();

        if (_header.FrameCount > 0)
        {
            int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
            byte[] frameBuffer = BufferPool.Rent(frameSize);

            try
            {
                bool continueReading = true;
                for (long i = 1; i <= _header.FrameCount && continueReading; i++)
                {
                    long framePosition = WalHeader.HEADER_SIZE + (i - 1) * frameSize;
                    _walStream.Position = framePosition;

                    int bytesRead = _walStream.Read(frameBuffer, 0, frameSize);
                    if (bytesRead < WalFrame.FRAME_HEADER_SIZE)
                    {
                        continueReading = false;
                    }
                    else
                    {
                        WalFrame frame = WalFrame.Deserialize(frameBuffer);

                        if (!frame.ValidateChecksum(_pageSize))
                        {
                            throw new InvalidOperationException($"WAL frame {i} has invalid checksum");
                        }

                        frames.Add(frame);
                    }
                }
            }
            finally
            {
                BufferPool.Return(frameBuffer);
            }
        }

        return frames;
    }

    public List<WalFrame> ReadFramesFromPosition(long startFrameNumber)
    {
        List<WalFrame> frames = new List<WalFrame>();

        if (_header.FrameCount > 0 && startFrameNumber <= _header.FrameCount)
        {
            long actualStart = startFrameNumber < 1 ? 1 : startFrameNumber;
            int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
            byte[] frameBuffer = BufferPool.Rent(frameSize);

            try
            {
                bool continueReading = true;
                for (long i = actualStart; i <= _header.FrameCount && continueReading; i++)
                {
                    long framePosition = WalHeader.HEADER_SIZE + (i - 1) * frameSize;
                    _walStream.Position = framePosition;

                    int bytesRead = _walStream.Read(frameBuffer, 0, frameSize);
                    if (bytesRead < WalFrame.FRAME_HEADER_SIZE)
                    {
                        continueReading = false;
                    }
                    else
                    {
                        WalFrame frame = WalFrame.Deserialize(frameBuffer);

                        if (!frame.ValidateChecksum(_pageSize))
                        {
                            throw new InvalidOperationException($"WAL frame {i} has invalid checksum");
                        }

                        frames.Add(frame);
                    }
                }
            }
            finally
            {
                BufferPool.Return(frameBuffer);
            }
        }

        return frames;
    }

    public void Flush()
    {
        lock (_writeLock)
        {
            _walStream.Flush(true);
        }
    }

    public void Truncate()
    {
        lock (_writeLock)
        {
            _walStream.SetLength(WalHeader.HEADER_SIZE);
            _header.FrameCount = 0;
            _currentFrameNumber = 0;
            WriteHeader();
            _walStream.Flush(true);
        }
    }

    public void UpdateCheckpointTxId(ulong txId)
    {
        lock (_writeLock)
        {
            _header.CheckpointTxId = txId;
            WriteHeader();
        }
    }

    public Dictionary<ulong, List<WalFrame>> GetFramesByTransaction()
    {
        Dictionary<ulong, List<WalFrame>> txFrames = new Dictionary<ulong, List<WalFrame>>();
        List<WalFrame> allFrames = ReadAllFrames();

        foreach (WalFrame frame in allFrames)
        {
            if (!txFrames.ContainsKey(frame.TxId))
            {
                txFrames[frame.TxId] = new List<WalFrame>();
            }

            txFrames[frame.TxId].Add(frame);
        }

        return txFrames;
    }

    public HashSet<ulong> GetCommittedTransactions()
    {
        HashSet<ulong> committed = new HashSet<ulong>();
        List<WalFrame> allFrames = ReadAllFrames();

        foreach (WalFrame frame in allFrames)
        {
            if (frame.IsCommit())
            {
                committed.Add(frame.TxId);
            }
        }

        return committed;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;

            if (_walStream != null)
            {
                _walStream.Flush();
                _walStream.Close();
                _walStream.Dispose();
                _walStream = null;
            }
        }
    }

    private void WriteHeader()
    {
        byte[] headerBytes = _header.Serialize();
        _walStream.Position = 0;
        _walStream.Write(headerBytes, 0, headerBytes.Length);
    }
}
