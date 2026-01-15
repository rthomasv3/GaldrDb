using System;
using System.Collections.Generic;
using System.IO;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.WAL;

internal class WriteAheadLog : IDisposable
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
            Salt1 = 1,
            Salt2 = GenerateRandomSalt()
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
            _header.Salt1 = 1;
            _header.Salt2 = GenerateRandomSalt();
            byte[] headerBuffer = BufferPool.Rent(WalHeader.HEADER_SIZE);
            try
            {
                _header.SerializeTo(headerBuffer);
                _walStream.Write(headerBuffer, 0, WalHeader.HEADER_SIZE);
                _walStream.Flush();
            }
            finally
            {
                BufferPool.Return(headerBuffer);
            }
            _currentFrameNumber = 0;
        }
        else
        {
            byte[] headerBuffer = BufferPool.Rent(WalHeader.HEADER_SIZE);
            try
            {
                _walStream.Position = 0;
                int bytesRead = _walStream.Read(headerBuffer, 0, WalHeader.HEADER_SIZE);

                if (bytesRead < WalHeader.HEADER_SIZE)
                {
                    throw new InvalidOperationException("WAL file is corrupted: header too short");
                }

                _header = WalHeader.Deserialize(headerBuffer);
            }
            finally
            {
                BufferPool.Return(headerBuffer);
            }

            if (!_header.ValidateMagicNumber())
            {
                throw new InvalidOperationException("WAL file is corrupted: invalid magic number");
            }

            if (_header.PageSize != _pageSize)
            {
                throw new InvalidOperationException($"WAL page size mismatch: expected {_pageSize}, found {_header.PageSize}");
            }

            // Calculate frame count from file size
            long dataSize = _walStream.Length - WalHeader.HEADER_SIZE;
            int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
            _currentFrameNumber = dataSize > 0 ? dataSize / frameSize : 0;
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
                Salt1 = _header.Salt1,
                Salt2 = _header.Salt2,
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

            return _currentFrameNumber;
        }
    }

    public void WriteFramesBatch(byte[] batchBuffer, int frameCount)
    {
        lock (_writeLock)
        {
            int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
            long startPosition = WalHeader.HEADER_SIZE + _currentFrameNumber * frameSize;

            _walStream.Position = startPosition;
            _walStream.Write(batchBuffer, 0, frameCount * frameSize);

            _currentFrameNumber += frameCount;
        }
    }

    public void WriteTransactionBatch(ulong txId, List<PendingPageWrite> pendingWrites)
    {
        if (pendingWrites.Count == 0)
        {
            return;
        }

        lock (_writeLock)
        {
            int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
            int totalSize = frameSize * pendingWrites.Count;
            byte[] batchBuffer = BufferPool.Rent(totalSize);

            try
            {
                int bufferOffset = 0;

                for (int i = 0; i < pendingWrites.Count; i++)
                {
                    _currentFrameNumber++;
                    PendingPageWrite write = pendingWrites[i];
                    bool isLastFrame = (i == pendingWrites.Count - 1);

                    WalFrame frame = new WalFrame
                    {
                        FrameNumber = _currentFrameNumber,
                        TxId = txId,
                        PageId = write.PageId,
                        PageType = write.PageType,
                        Flags = isLastFrame ? WalFrameFlags.Commit : WalFrameFlags.None,
                        Salt1 = _header.Salt1,
                        Salt2 = _header.Salt2,
                        Data = write.Data ?? Array.Empty<byte>()
                    };

                    // Serialize frame header + data
                    byte[] frameBuffer = BufferPool.Rent(frameSize);
                    try
                    {
                        int bytesWritten = frame.SerializeTo(frameBuffer);

                        // Copy to batch buffer
                        Array.Copy(frameBuffer, 0, batchBuffer, bufferOffset, bytesWritten);

                        // Pad to full frame size if data is smaller
                        int paddingNeeded = frameSize - bytesWritten;
                        if (paddingNeeded > 0)
                        {
                            Array.Clear(batchBuffer, bufferOffset + bytesWritten, paddingNeeded);
                        }
                    }
                    finally
                    {
                        BufferPool.Return(frameBuffer);
                    }

                    bufferOffset += frameSize;
                }

                // Write all frames in a single I/O operation
                long startPosition = WalHeader.HEADER_SIZE + (_currentFrameNumber - pendingWrites.Count) * frameSize;
                _walStream.Position = startPosition;
                _walStream.Write(batchBuffer, 0, totalSize);
            }
            finally
            {
                BufferPool.Return(batchBuffer);
            }
        }
    }

    public List<WalFrame> ReadAllFrames()
    {
        List<WalFrame> frames = new List<WalFrame>();
        int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
        byte[] frameBuffer = BufferPool.Rent(frameSize);

        try
        {
            long framePosition = WalHeader.HEADER_SIZE;

            while (true)
            {
                _walStream.Position = framePosition;
                int bytesRead = _walStream.Read(frameBuffer, 0, frameSize);

                // Stop at EOF
                if (bytesRead < WalFrame.FRAME_HEADER_SIZE)
                {
                    break;
                }

                WalFrame frame = WalFrame.Deserialize(frameBuffer);

                // Stop at salt mismatch (stale data from before checkpoint)
                if (frame.Salt1 != _header.Salt1 || frame.Salt2 != _header.Salt2)
                {
                    break;
                }

                // Stop at bad checksum (corruption or partial write)
                if (!frame.ValidateChecksum(_pageSize))
                {
                    break;
                }

                frames.Add(frame);
                framePosition += frameSize;
            }
        }
        finally
        {
            BufferPool.Return(frameBuffer);
        }

        return frames;
    }

    public List<WalFrame> ReadFramesFromPosition(long startFrameNumber)
    {
        List<WalFrame> frames = new List<WalFrame>();
        long actualStart = startFrameNumber < 1 ? 1 : startFrameNumber;
        int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
        byte[] frameBuffer = BufferPool.Rent(frameSize);

        try
        {
            long framePosition = WalHeader.HEADER_SIZE + (actualStart - 1) * frameSize;

            while (true)
            {
                _walStream.Position = framePosition;
                int bytesRead = _walStream.Read(frameBuffer, 0, frameSize);

                // Stop at EOF
                if (bytesRead < WalFrame.FRAME_HEADER_SIZE)
                {
                    break;
                }

                WalFrame frame = WalFrame.Deserialize(frameBuffer);

                // Stop at salt mismatch (stale data from before checkpoint)
                if (frame.Salt1 != _header.Salt1 || frame.Salt2 != _header.Salt2)
                {
                    break;
                }

                // Stop at bad checksum (corruption or partial write)
                if (!frame.ValidateChecksum(_pageSize))
                {
                    break;
                }

                frames.Add(frame);
                framePosition += frameSize;
            }
        }
        finally
        {
            BufferPool.Return(frameBuffer);
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
            _currentFrameNumber = 0;
            _header.Salt1++;
            _header.Salt2 = GenerateRandomSalt();
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
        byte[] headerBuffer = BufferPool.Rent(WalHeader.HEADER_SIZE);
        try
        {
            _header.SerializeTo(headerBuffer);
            _walStream.Position = 0;
            _walStream.Write(headerBuffer, 0, WalHeader.HEADER_SIZE);
        }
        finally
        {
            BufferPool.Return(headerBuffer);
        }
    }

    private static uint GenerateRandomSalt()
    {
        return (uint)Random.Shared.Next();
    }
}
