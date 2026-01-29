using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;
using Microsoft.Win32.SafeHandles;

namespace GaldrDbEngine.IO;

internal sealed class EncryptedPageIO : IPageIO
{
    private readonly int _pageSize;
    private readonly int _usableSize;
    private readonly byte[] _encryptionKey;
    private readonly SafeFileHandle _fileHandle;
    private readonly AsyncReaderWriterLock _rwLock;
    private bool _disposed;

    private const int NONCE_SIZE = 12;
    private const int TAG_SIZE = 16;
    private const int RESERVE_SIZE = 32;
    private const int KEY_SIZE = 32;

    private EncryptedPageIO(SafeFileHandle fileHandle, int pageSize, byte[] encryptionKey)
    {
        _fileHandle = fileHandle;
        _pageSize = pageSize;
        _usableSize = pageSize - RESERVE_SIZE;
        _encryptionKey = encryptionKey;
        _rwLock = new AsyncReaderWriterLock();
        _disposed = false;
    }

    public static EncryptedPageIO Create(string filePath, int pageSize, EncryptionOptions options, out byte[] encryptionKey)
    {
        SafeFileHandle fileHandle = File.OpenHandle(filePath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);

        EncryptionHeader header = EncryptionHeader.Create(options.KdfIterations);
        header.PageSize = pageSize;
        byte[] headerBuffer = new byte[EncryptionHeader.HEADER_SIZE];
        header.SerializeTo(headerBuffer);

        RandomAccess.Write(fileHandle, headerBuffer, 0);

        encryptionKey = DeriveKey(options.Password, header.Salt, header.KdfIterations);

        return new EncryptedPageIO(fileHandle, pageSize, encryptionKey);
    }

    public static EncryptedPageIO Open(string filePath, int pageSize, EncryptionOptions options, out byte[] encryptionKey)
    {
        SafeFileHandle fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
        EncryptedPageIO result = null;
        Exception failure = null;
        encryptionKey = null;

        byte[] headerBuffer = new byte[EncryptionHeader.HEADER_SIZE];
        int bytesRead = RandomAccess.Read(fileHandle, headerBuffer, 0);

        if (bytesRead < EncryptionHeader.HEADER_SIZE)
        {
            failure = new InvalidOperationException("File is too small to contain encryption header");
        }
        else
        {
            EncryptionHeader header = EncryptionHeader.Deserialize(headerBuffer);

            if (!header.IsValid())
            {
                failure = new InvalidOperationException("File does not appear to be encrypted or is corrupted");
            }
            else
            {
                byte[] key = DeriveKey(options.Password, header.Salt, header.KdfIterations);
                EncryptedPageIO pageIO = new EncryptedPageIO(fileHandle, pageSize, key);

                if (!pageIO.ValidatePassword())
                {
                    CryptographicOperations.ZeroMemory(key);
                    pageIO.Dispose();
                    failure = new InvalidPasswordException();
                }
                else
                {
                    result = pageIO;
                    encryptionKey = key;
                }
            }
        }

        if (failure != null)
        {
            if (result == null)
            {
                fileHandle.Dispose();
            }
            throw failure;
        }

        return result;
    }

    public static bool IsEncryptedFile(string filePath)
    {
        bool result = false;

        if (File.Exists(filePath))
        {
            using (SafeFileHandle handle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                long fileLength = RandomAccess.GetLength(handle);

                if (fileLength >= EncryptionHeader.HEADER_SIZE)
                {
                    byte[] headerBuffer = new byte[EncryptionHeader.HEADER_SIZE];
                    int bytesRead = RandomAccess.Read(handle, headerBuffer, 0);

                    if (bytesRead == EncryptionHeader.HEADER_SIZE)
                    {
                        EncryptionHeader header = EncryptionHeader.Deserialize(headerBuffer);
                        result = header.IsValid();
                    }
                }
            }
        }

        return result;
    }

    public static int GetPageSize(string filePath)
    {
        int pageSize = 0;

        using (SafeFileHandle handle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            byte[] headerBuffer = new byte[EncryptionHeader.HEADER_SIZE];
            int bytesRead = RandomAccess.Read(handle, headerBuffer, 0);

            if (bytesRead == EncryptionHeader.HEADER_SIZE)
            {
                EncryptionHeader header = EncryptionHeader.Deserialize(headerBuffer);
                if (header.IsValid())
                {
                    pageSize = header.PageSize;
                }
            }
        }

        return pageSize;
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        if (destination.Length < _pageSize)
        {
            throw new ArgumentException($"Destination length {destination.Length} is smaller than page size {_pageSize}");
        }

        long offset = EncryptionHeader.HEADER_SIZE + (long)pageId * _pageSize;
        byte[] encryptedBuffer = BufferPool.Rent(_pageSize);

        try
        {
            _rwLock.EnterReadLock();
            try
            {
                int totalBytesRead = 0;

                while (totalBytesRead < _pageSize)
                {
                    int bytesRead = RandomAccess.Read(_fileHandle, encryptedBuffer.AsSpan(totalBytesRead, _pageSize - totalBytesRead), offset + totalBytesRead);

                    if (bytesRead == 0)
                    {
                        break;
                    }

                    totalBytesRead += bytesRead;
                }

                if (totalBytesRead == 0 || IsUnwrittenPage(encryptedBuffer.AsSpan(0, _pageSize)))
                {
                    destination.Slice(0, _pageSize).Clear();
                }
                else
                {
                    DecryptPage(encryptedBuffer, destination);
                }
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }
        finally
        {
            BufferPool.Return(encryptedBuffer);
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        if (data.Length != _pageSize)
        {
            throw new ArgumentException($"Data length {data.Length} does not match page size {_pageSize}");
        }

        long offset = EncryptionHeader.HEADER_SIZE + (long)pageId * _pageSize;
        byte[] encryptedBuffer = BufferPool.Rent(_pageSize);

        try
        {
            EncryptPage(data, encryptedBuffer);

            _rwLock.EnterWriteLock();
            try
            {
                RandomAccess.Write(_fileHandle, encryptedBuffer.AsSpan(0, _pageSize), offset);
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }
        finally
        {
            BufferPool.Return(encryptedBuffer);
        }
    }

    public async Task ReadPageAsync(int pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        if (destination.Length < _pageSize)
        {
            throw new ArgumentException($"Destination length {destination.Length} is smaller than page size {_pageSize}");
        }

        long offset = EncryptionHeader.HEADER_SIZE + (long)pageId * _pageSize;
        byte[] encryptedBuffer = BufferPool.Rent(_pageSize);

        try
        {
            await _rwLock.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                int totalBytesRead = 0;

                while (totalBytesRead < _pageSize)
                {
                    int bytesRead = await RandomAccess.ReadAsync(_fileHandle, encryptedBuffer.AsMemory(totalBytesRead, _pageSize - totalBytesRead), offset + totalBytesRead, cancellationToken).ConfigureAwait(false);

                    if (bytesRead == 0)
                    {
                        break;
                    }

                    totalBytesRead += bytesRead;
                }

                if (totalBytesRead == 0 || IsUnwrittenPage(encryptedBuffer.AsSpan(0, _pageSize)))
                {
                    destination.Span.Slice(0, _pageSize).Clear();
                }
                else
                {
                    DecryptPage(encryptedBuffer, destination.Span);
                }
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }
        finally
        {
            BufferPool.Return(encryptedBuffer);
        }
    }

    public async Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        if (data.Length != _pageSize)
        {
            throw new ArgumentException($"Data length {data.Length} does not match page size {_pageSize}");
        }

        long offset = EncryptionHeader.HEADER_SIZE + (long)pageId * _pageSize;
        byte[] encryptedBuffer = BufferPool.Rent(_pageSize);

        try
        {
            EncryptPage(data.Span, encryptedBuffer);

            await _rwLock.EnterWriteLockAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await RandomAccess.WriteAsync(_fileHandle, encryptedBuffer.AsMemory(0, _pageSize), offset, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }
        finally
        {
            BufferPool.Return(encryptedBuffer);
        }
    }

    public void Flush()
    {
        _rwLock.EnterWriteLock();
        try
        {
            RandomAccess.FlushToDisk(_fileHandle);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        Flush();
        return Task.CompletedTask;
    }

    public void SetLength(long newSize)
    {
        long adjustedSize = EncryptionHeader.HEADER_SIZE + newSize;

        _rwLock.EnterWriteLock();
        try
        {
            RandomAccess.SetLength(_fileHandle, adjustedSize);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    public void Close()
    {
        if (!_disposed)
        {
            _fileHandle.Close();
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _rwLock.Dispose();
            _fileHandle.Dispose();
            // Note: Key is owned and zeroed by GaldrDb, not here
        }
    }

    private void EncryptPage(ReadOnlySpan<byte> plaintext, Span<byte> ciphertext)
    {
        Span<byte> nonce = ciphertext.Slice(_usableSize, NONCE_SIZE);
        Span<byte> tag = ciphertext.Slice(_usableSize + NONCE_SIZE, TAG_SIZE);
        Span<byte> encryptedData = ciphertext.Slice(0, _usableSize);

        RandomNumberGenerator.Fill(nonce);

        using (AesGcm aes = new AesGcm(_encryptionKey, TAG_SIZE))
        {
            aes.Encrypt(nonce, plaintext.Slice(0, _usableSize), encryptedData, tag);
        }

        ciphertext.Slice(_usableSize + NONCE_SIZE + TAG_SIZE, RESERVE_SIZE - NONCE_SIZE - TAG_SIZE).Clear();
    }

    private void DecryptPage(ReadOnlySpan<byte> ciphertext, Span<byte> plaintext)
    {
        ReadOnlySpan<byte> nonce = ciphertext.Slice(_usableSize, NONCE_SIZE);
        ReadOnlySpan<byte> tag = ciphertext.Slice(_usableSize + NONCE_SIZE, TAG_SIZE);
        ReadOnlySpan<byte> encryptedData = ciphertext.Slice(0, _usableSize);

        using (AesGcm aes = new AesGcm(_encryptionKey, TAG_SIZE))
        {
            aes.Decrypt(nonce, encryptedData, tag, plaintext.Slice(0, _usableSize));
        }

        plaintext.Slice(_usableSize, RESERVE_SIZE).Clear();
    }

    private static bool IsUnwrittenPage(ReadOnlySpan<byte> pageData)
    {
        bool allZeros = true;

        for (int i = 0; i < pageData.Length && allZeros; i++)
        {
            if (pageData[i] != 0)
            {
                allZeros = false;
            }
        }

        return allZeros;
    }

    private bool ValidatePassword()
    {
        bool valid = false;
        byte[] buffer = BufferPool.Rent(_pageSize);

        try
        {
            long offset = EncryptionHeader.HEADER_SIZE;
            int bytesRead = RandomAccess.Read(_fileHandle, buffer.AsSpan(0, _pageSize), offset);

            if (bytesRead >= _pageSize)
            {
                try
                {
                    byte[] decrypted = new byte[_pageSize];
                    DecryptPage(buffer, decrypted);

                    uint magic = BinaryHelper.ReadUInt32LE(decrypted, 0);
                    valid = (magic == PageConstants.MAGIC_NUMBER);
                }
                catch (CryptographicException)
                {
                    valid = false;
                }
            }
            else
            {
                // No page 0 data to validate - file is incomplete or corrupted
                valid = false;
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }

        return valid;
    }

    private static byte[] DeriveKey(string password, byte[] salt, int iterations)
    {
        return Rfc2898DeriveBytes.Pbkdf2(password, salt, iterations, HashAlgorithmName.SHA512, KEY_SIZE);
    }
}
