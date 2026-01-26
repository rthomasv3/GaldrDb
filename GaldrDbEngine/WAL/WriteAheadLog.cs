using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.WAL;

internal class WriteAheadLog : IDisposable
{
    private readonly string _walPath;
    private readonly int _pageSize;
    private readonly object _writeLock;
    private IWalStreamIO _streamIO;
    private WalHeader _header;
    private long _currentFrameNumber;
    private bool _disposed;

    // Optional injection points for simulation testing (null in production)
    internal IWalStreamIO _testStreamIO;
    internal Func<uint> _testSaltGenerator;

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
        if (_testStreamIO != null)
        {
            _streamIO = _testStreamIO;
        }
        else
        {
            if (File.Exists(_walPath))
            {
                throw new InvalidOperationException($"WAL file already exists: {_walPath}");
            }

            _streamIO = new FileWalStreamIO(_walPath, createNew: true);
        }

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
        if (_testStreamIO != null)
        {
            _streamIO = _testStreamIO;
        }
        else
        {
            if (!File.Exists(_walPath))
            {
                throw new FileNotFoundException($"WAL file not found: {_walPath}");
            }

            _streamIO = new FileWalStreamIO(_walPath, createNew: false);
        }

        // Handle empty WAL file (can occur after truncation or crash during creation)
        if (_streamIO.Length == 0)
        {
            _header = new WalHeader();
            _header.PageSize = _pageSize;
            _header.Salt1 = 1;
            _header.Salt2 = GenerateRandomSalt();
            byte[] headerBuffer = BufferPool.Rent(WalHeader.HEADER_SIZE);
            try
            {
                _header.SerializeTo(headerBuffer);
                _streamIO.WriteAtPosition(0, headerBuffer.AsSpan(0, WalHeader.HEADER_SIZE));
                _streamIO.Flush();
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
                int bytesRead = _streamIO.ReadAtPosition(0, headerBuffer.AsSpan(0, WalHeader.HEADER_SIZE));

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
            long dataSize = _streamIO.Length - WalHeader.HEADER_SIZE;
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

            int frameBufferSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
            byte[] frameBuffer = BufferPool.Rent(frameBufferSize);
            try
            {
                int bytesWritten = WalFrame.SerializeFrameTo(
                    frameBuffer,
                    0,
                    _currentFrameNumber,
                    txId,
                    pageId,
                    pageType,
                    flags,
                    _header.Salt1,
                    _header.Salt2,
                    frameData);

                long framePosition = WalHeader.HEADER_SIZE + (_currentFrameNumber - 1) * (WalFrame.FRAME_HEADER_SIZE + _pageSize);

                // Pad to full frame size if data is smaller
                int paddingNeeded = _pageSize - frameData.Length;
                if (paddingNeeded > 0)
                {
                    Array.Clear(frameBuffer, bytesWritten, paddingNeeded);
                }

                int totalWriteSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
                _streamIO.WriteAtPosition(framePosition, frameBuffer.AsSpan(0, totalWriteSize));
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

            _streamIO.WriteAtPosition(startPosition, batchBuffer.AsSpan(0, frameCount * frameSize));

            _currentFrameNumber += frameCount;
        }
    }

    public long WriteFrameEntries(ulong txId, List<WalFrameEntry> entries)
    {
        long startingFrameNumber = 0;

        if (entries.Count > 0)
        {
            lock (_writeLock)
            {
                int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
                int totalSize = frameSize * entries.Count;
                byte[] batchBuffer = BufferPool.Rent(totalSize);

                try
                {
                    int bufferOffset = 0;
                    startingFrameNumber = _currentFrameNumber + 1;

                    for (int i = 0; i < entries.Count; i++)
                    {
                        _currentFrameNumber++;
                        WalFrameEntry entry = entries[i];
                        bool isLastFrame = (i == entries.Count - 1);
                        WalFrameFlags flags = isLastFrame ? WalFrameFlags.Commit : WalFrameFlags.None;
                        byte[] data = entry.Data ?? Array.Empty<byte>();

                        int bytesWritten = WalFrame.SerializeFrameTo(
                            batchBuffer,
                            bufferOffset,
                            _currentFrameNumber,
                            txId,
                            entry.PageId,
                            entry.PageType,
                            flags,
                            _header.Salt1,
                            _header.Salt2,
                            data);

                        // Pad to full frame size if data is smaller
                        int paddingNeeded = frameSize - bytesWritten;
                        if (paddingNeeded > 0)
                        {
                            Array.Clear(batchBuffer, bufferOffset + bytesWritten, paddingNeeded);
                        }

                        bufferOffset += frameSize;
                    }

                    // Write all frames in a single I/O operation
                    long startPosition = WalHeader.HEADER_SIZE + (_currentFrameNumber - entries.Count) * frameSize;
                    _streamIO.WriteAtPosition(startPosition, batchBuffer.AsSpan(0, totalSize));
                }
                finally
                {
                    BufferPool.Return(batchBuffer);
                }
            }
        }

        return startingFrameNumber;
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
                int bytesRead = _streamIO.ReadAtPosition(framePosition, frameBuffer.AsSpan(0, frameSize));

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
                int bytesRead = _streamIO.ReadAtPosition(framePosition, frameBuffer.AsSpan(0, frameSize));

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
            _streamIO.Flush();
        }
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return _streamIO.FlushAsync(cancellationToken);
    }

    public bool ReadFrameData(long frameNumber, Span<byte> destination)
    {
        bool success = false;

        if (frameNumber >= 1)
        {
            int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
            long framePosition = WalHeader.HEADER_SIZE + (frameNumber - 1) * frameSize;

            byte[] frameBuffer = BufferPool.Rent(frameSize);
            try
            {
                int bytesRead = _streamIO.ReadAtPosition(framePosition, frameBuffer.AsSpan(0, frameSize));

                if (bytesRead >= frameSize)
                {
                    // Read header fields directly without allocating WalFrame
                    WalFrame.ReadSaltsFromBuffer(frameBuffer, out uint salt1, out uint salt2);

                    if (salt1 == _header.Salt1 && salt2 == _header.Salt2)
                    {
                        // Validate checksum directly on buffer (no re-serialization)
                        if (WalFrame.ValidateChecksumInBuffer(frameBuffer, _pageSize))
                        {
                            // Copy data directly from buffer to destination
                            int dataLength = WalFrame.ReadDataLengthFromBuffer(frameBuffer);
                            int copyLength = Math.Min(dataLength, destination.Length);
                            frameBuffer.AsSpan(WalFrame.FRAME_HEADER_SIZE, copyLength).CopyTo(destination);
                            success = true;
                        }
                    }
                }
            }
            finally
            {
                BufferPool.Return(frameBuffer);
            }
        }

        return success;
    }

    public async Task<bool> ReadFrameDataAsync(long frameNumber, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        bool success = false;

        if (frameNumber >= 1)
        {
            int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
            long framePosition = WalHeader.HEADER_SIZE + (frameNumber - 1) * frameSize;

            byte[] frameBuffer = BufferPool.Rent(frameSize);
            try
            {
                int bytesRead = await _streamIO.ReadAtPositionAsync(framePosition, frameBuffer.AsMemory(0, frameSize), cancellationToken).ConfigureAwait(false);

                if (bytesRead >= frameSize)
                {
                    // Read header fields directly without allocating WalFrame
                    WalFrame.ReadSaltsFromBuffer(frameBuffer, out uint salt1, out uint salt2);

                    if (salt1 == _header.Salt1 && salt2 == _header.Salt2)
                    {
                        // Validate checksum directly on buffer (no re-serialization)
                        if (WalFrame.ValidateChecksumInBuffer(frameBuffer, _pageSize))
                        {
                            // Copy data directly from buffer to destination
                            int dataLength = WalFrame.ReadDataLengthFromBuffer(frameBuffer);
                            int copyLength = Math.Min(dataLength, destination.Length);
                            frameBuffer.AsMemory(WalFrame.FRAME_HEADER_SIZE, copyLength).CopyTo(destination);
                            success = true;
                        }
                    }
                }
            }
            finally
            {
                BufferPool.Return(frameBuffer);
            }
        }

        return success;
    }

    public void Truncate()
    {
        lock (_writeLock)
        {
            _streamIO.SetLength(WalHeader.HEADER_SIZE);
            _currentFrameNumber = 0;
            _header.Salt1++;
            _header.Salt2 = GenerateRandomSalt();
            WriteHeader();
            _streamIO.Flush();
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

            if (_streamIO != null)
            {
                _streamIO.Flush();
                _streamIO.Dispose();
                _streamIO = null;
            }
        }
    }

    private void WriteHeader()
    {
        byte[] headerBuffer = BufferPool.Rent(WalHeader.HEADER_SIZE);
        try
        {
            _header.SerializeTo(headerBuffer);
            _streamIO.WriteAtPosition(0, headerBuffer.AsSpan(0, WalHeader.HEADER_SIZE));
        }
        finally
        {
            BufferPool.Return(headerBuffer);
        }
    }

    private uint GenerateRandomSalt()
    {
        uint result;

        if (_testSaltGenerator != null)
        {
            result = _testSaltGenerator();
        }
        else
        {
            result = (uint)Random.Shared.Next();
        }

        return result;
    }
}
