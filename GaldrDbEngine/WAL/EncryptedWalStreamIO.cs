using System;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.WAL;

internal sealed class EncryptedWalStreamIO : IWalStreamIO
{
    private readonly IWalStreamIO _innerStreamIO;
    private readonly byte[] _encryptionKey;
    private readonly int _pageSize;
    private bool _disposed;

    private const int NONCE_SIZE = 12;
    private const int TAG_SIZE = 16;

    public EncryptedWalStreamIO(IWalStreamIO innerStreamIO, byte[] encryptionKey, int pageSize)
    {
        _innerStreamIO = innerStreamIO;
        _encryptionKey = encryptionKey;
        _pageSize = pageSize;
        _disposed = false;
    }

    public long Length
    {
        get { return _innerStreamIO.Length; }
    }

    public int ReadAtPosition(long position, Span<byte> buffer)
    {
        int bytesRead = _innerStreamIO.ReadAtPosition(position, buffer);

        if (bytesRead > 0 && IsFrameData(position))
        {
            DecryptFramePayload(position, buffer.Slice(0, bytesRead));
        }

        return bytesRead;
    }

    public async Task<int> ReadAtPositionAsync(long position, Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        int bytesRead = await _innerStreamIO.ReadAtPositionAsync(position, buffer, cancellationToken).ConfigureAwait(false);

        if (bytesRead > 0 && IsFrameData(position))
        {
            DecryptFramePayload(position, buffer.Span.Slice(0, bytesRead));
        }

        return bytesRead;
    }

    public void WriteAtPosition(long position, ReadOnlySpan<byte> buffer)
    {
        if (IsFrameData(position) && buffer.Length > WalFrame.FRAME_HEADER_SIZE)
        {
            int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
            byte[] encrypted = BufferPool.Rent(buffer.Length);
            try
            {
                buffer.CopyTo(encrypted);

                // Handle batch writes - encrypt each frame individually
                int offset = 0;
                while (offset + frameSize <= buffer.Length)
                {
                    EncryptFramePayload(position + offset, encrypted.AsSpan(offset, frameSize));
                    offset += frameSize;
                }

                _innerStreamIO.WriteAtPosition(position, encrypted.AsSpan(0, buffer.Length));
            }
            finally
            {
                BufferPool.Return(encrypted);
            }
        }
        else
        {
            _innerStreamIO.WriteAtPosition(position, buffer);
        }
    }

    public async Task WriteAtPositionAsync(long position, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (IsFrameData(position) && buffer.Length > WalFrame.FRAME_HEADER_SIZE)
        {
            int frameSize = WalFrame.FRAME_HEADER_SIZE + _pageSize;
            byte[] encrypted = BufferPool.Rent(buffer.Length);
            try
            {
                buffer.CopyTo(encrypted);

                // Handle batch writes - encrypt each frame individually
                int offset = 0;
                while (offset + frameSize <= buffer.Length)
                {
                    EncryptFramePayload(position + offset, encrypted.AsSpan(offset, frameSize));
                    offset += frameSize;
                }

                await _innerStreamIO.WriteAtPositionAsync(position, encrypted.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                BufferPool.Return(encrypted);
            }
        }
        else
        {
            await _innerStreamIO.WriteAtPositionAsync(position, buffer, cancellationToken).ConfigureAwait(false);
        }
    }

    public void Flush()
    {
        _innerStreamIO.Flush();
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return _innerStreamIO.FlushAsync(cancellationToken);
    }

    public void SetLength(long length)
    {
        _innerStreamIO.SetLength(length);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _innerStreamIO.Dispose();
        }
    }

    private bool IsFrameData(long position)
    {
        return position >= WalHeader.HEADER_SIZE;
    }

    private void EncryptFramePayload(long framePosition, Span<byte> frameBuffer)
    {
        int payloadOffset = WalFrame.FRAME_HEADER_SIZE;
        int payloadLength = frameBuffer.Length - payloadOffset;

        if (payloadLength > PageConstants.ENCRYPTION_RESERVE_SIZE)
        {
            int usableLength = payloadLength - PageConstants.ENCRYPTION_RESERVE_SIZE;
            Span<byte> payload = frameBuffer.Slice(payloadOffset, usableLength);
            Span<byte> nonce = frameBuffer.Slice(payloadOffset + usableLength, NONCE_SIZE);
            Span<byte> tag = frameBuffer.Slice(payloadOffset + usableLength + NONCE_SIZE, TAG_SIZE);

            RandomNumberGenerator.Fill(nonce);

            byte[] temp = BufferPool.Rent(usableLength);
            try
            {
                using (AesGcm aes = new AesGcm(_encryptionKey, TAG_SIZE))
                {
                    aes.Encrypt(nonce, payload, temp.AsSpan(0, usableLength), tag);
                    temp.AsSpan(0, usableLength).CopyTo(payload);
                }
            }
            finally
            {
                BufferPool.Return(temp);
            }

            frameBuffer.Slice(payloadOffset + usableLength + NONCE_SIZE + TAG_SIZE, PageConstants.ENCRYPTION_RESERVE_SIZE - NONCE_SIZE - TAG_SIZE).Clear();
        }
    }

    private void DecryptFramePayload(long framePosition, Span<byte> frameBuffer)
    {
        if (frameBuffer.Length > WalFrame.FRAME_HEADER_SIZE)
        {
            int payloadOffset = WalFrame.FRAME_HEADER_SIZE;
            int payloadLength = frameBuffer.Length - payloadOffset;

            if (payloadLength > PageConstants.ENCRYPTION_RESERVE_SIZE)
            {
                int usableLength = payloadLength - PageConstants.ENCRYPTION_RESERVE_SIZE;
                Span<byte> payload = frameBuffer.Slice(payloadOffset, usableLength);
                ReadOnlySpan<byte> nonce = frameBuffer.Slice(payloadOffset + usableLength, NONCE_SIZE);
                ReadOnlySpan<byte> tag = frameBuffer.Slice(payloadOffset + usableLength + NONCE_SIZE, TAG_SIZE);

                byte[] temp = BufferPool.Rent(usableLength);
                try
                {
                    using (AesGcm aes = new AesGcm(_encryptionKey, TAG_SIZE))
                    {
                        aes.Decrypt(nonce, payload, tag, temp.AsSpan(0, usableLength));
                        temp.AsSpan(0, usableLength).CopyTo(payload);
                    }

                    frameBuffer.Slice(payloadOffset + usableLength, PageConstants.ENCRYPTION_RESERVE_SIZE).Clear();
                }
                finally
                {
                    BufferPool.Return(temp);
                }
            }
        }
    }

}
