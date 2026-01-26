using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class LruPageCacheTests
{
    private const int PAGE_SIZE = 4096;

    [TestMethod]
    public void ReadPage_CacheMiss_ReadsFromInnerIO()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);
        byte[] testData = CreateTestData(PAGE_SIZE, 42);
        innerIO.WritePage(0, testData);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        byte[] result = new byte[PAGE_SIZE];
        cache.ReadPage(0, result);

        CollectionAssert.AreEqual(testData, result);
    }

    [TestMethod]
    public void ReadPage_CacheHit_ReturnsCorrectData()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);
        byte[] testData = CreateTestData(PAGE_SIZE, 42);
        innerIO.WritePage(0, testData);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        byte[] result1 = new byte[PAGE_SIZE];
        cache.ReadPage(0, result1);

        byte[] result2 = new byte[PAGE_SIZE];
        cache.ReadPage(0, result2);

        CollectionAssert.AreEqual(testData, result1);
        CollectionAssert.AreEqual(testData, result2);
    }

    [TestMethod]
    public void WritePage_UpdatesCache_SubsequentReadReturnsCachedData()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        byte[] testData = CreateTestData(PAGE_SIZE, 99);
        cache.WritePage(0, testData);

        byte[] result = new byte[PAGE_SIZE];
        cache.ReadPage(0, result);

        CollectionAssert.AreEqual(testData, result);
    }

    [TestMethod]
    public void ReadPage_Eviction_EvictedPageReadsFromDisk()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        byte[] page0Data = CreateTestData(PAGE_SIZE, 0);
        byte[] page1Data = CreateTestData(PAGE_SIZE, 1);
        innerIO.WritePage(0, page0Data);
        innerIO.WritePage(1, page1Data);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 1);

        byte[] buffer = new byte[PAGE_SIZE];
        cache.ReadPage(0, buffer);
        CollectionAssert.AreEqual(page0Data, buffer);

        cache.ReadPage(1, buffer);
        CollectionAssert.AreEqual(page1Data, buffer);

        byte[] modifiedPage0 = CreateTestData(PAGE_SIZE, 111);
        innerIO.WritePage(0, modifiedPage0);

        cache.ReadPage(0, buffer);

        CollectionAssert.AreEqual(modifiedPage0, buffer);
    }

    [TestMethod]
    public void ReadPage_MultiplePages_AllReturnCorrectData()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        for (int i = 0; i < 10; i++)
        {
            byte[] data = CreateTestData(PAGE_SIZE, (byte)(i * 10));
            innerIO.WritePage(i, data);
        }

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 20);

        for (int i = 0; i < 10; i++)
        {
            byte[] expected = CreateTestData(PAGE_SIZE, (byte)(i * 10));
            byte[] result = new byte[PAGE_SIZE];
            cache.ReadPage(i, result);

            CollectionAssert.AreEqual(expected, result);
        }
    }

    [TestMethod]
    public async Task ReadPageAsync_CacheMiss_ReadsFromInnerIO()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);
        byte[] testData = CreateTestData(PAGE_SIZE, 42);
        innerIO.WritePage(0, testData);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        byte[] result = new byte[PAGE_SIZE];
        await cache.ReadPageAsync(0, result);

        CollectionAssert.AreEqual(testData, result);
    }

    [TestMethod]
    public async Task ReadPageAsync_CacheHit_ReturnsCorrectData()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);
        byte[] testData = CreateTestData(PAGE_SIZE, 42);
        innerIO.WritePage(0, testData);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        byte[] result1 = new byte[PAGE_SIZE];
        await cache.ReadPageAsync(0, result1);

        byte[] result2 = new byte[PAGE_SIZE];
        await cache.ReadPageAsync(0, result2);

        CollectionAssert.AreEqual(testData, result1);
        CollectionAssert.AreEqual(testData, result2);
    }

    [TestMethod]
    public async Task ReadPageAsync_ConcurrentReads_AllReturnCorrectData()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        for (int i = 0; i < 10; i++)
        {
            byte[] data = CreateTestData(PAGE_SIZE, (byte)(i * 10));
            innerIO.WritePage(i, data);
        }

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 20);

        List<Task> tasks = new List<Task>();

        for (int iteration = 0; iteration < 5; iteration++)
        {
            for (int pageId = 0; pageId < 10; pageId++)
            {
                int capturedPageId = pageId;
                Task task = Task.Run(async () =>
                {
                    byte[] expected = CreateTestData(PAGE_SIZE, (byte)(capturedPageId * 10));
                    byte[] result = new byte[PAGE_SIZE];
                    await cache.ReadPageAsync(capturedPageId, result);

                    CollectionAssert.AreEqual(expected, result);
                });
                tasks.Add(task);
            }
        }

        await Task.WhenAll(tasks);
    }

    [TestMethod]
    public async Task ReadPageAsync_ConcurrentReadsSamePage_AllReturnCorrectData()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);
        byte[] testData = CreateTestData(PAGE_SIZE, 77);
        innerIO.WritePage(0, testData);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        List<Task> tasks = new List<Task>();

        for (int i = 0; i < 50; i++)
        {
            Task task = Task.Run(async () =>
            {
                byte[] result = new byte[PAGE_SIZE];
                await cache.ReadPageAsync(0, result);

                CollectionAssert.AreEqual(testData, result);
            });
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);
    }

    [TestMethod]
    public async Task WritePageAsync_UpdatesCache_SubsequentReadReturnsCachedData()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        byte[] testData = CreateTestData(PAGE_SIZE, 99);
        await cache.WritePageAsync(0, testData);

        byte[] result = new byte[PAGE_SIZE];
        await cache.ReadPageAsync(0, result);

        CollectionAssert.AreEqual(testData, result);
    }

    [TestMethod]
    public async Task ReadPageAsync_Eviction_EvictedPageReadsFromDisk()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        byte[] page0Data = CreateTestData(PAGE_SIZE, 0);
        byte[] page1Data = CreateTestData(PAGE_SIZE, 1);
        innerIO.WritePage(0, page0Data);
        innerIO.WritePage(1, page1Data);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 1);

        byte[] buffer = new byte[PAGE_SIZE];
        await cache.ReadPageAsync(0, buffer);
        CollectionAssert.AreEqual(page0Data, buffer);

        await cache.ReadPageAsync(1, buffer);
        CollectionAssert.AreEqual(page1Data, buffer);

        byte[] modifiedPage0 = CreateTestData(PAGE_SIZE, 111);
        innerIO.WritePage(0, modifiedPage0);

        await cache.ReadPageAsync(0, buffer);

        CollectionAssert.AreEqual(modifiedPage0, buffer);
    }

    [TestMethod]
    public async Task ReadPageAsync_ConcurrentReadsWithEviction_NoCorruption()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        for (int i = 0; i < 20; i++)
        {
            byte[] data = CreateTestData(PAGE_SIZE, (byte)i);
            innerIO.WritePage(i, data);
        }

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 5);

        List<Task> tasks = new List<Task>();
        Random random = new Random(42);

        for (int i = 0; i < 100; i++)
        {
            int pageId = random.Next(0, 20);
            int capturedPageId = pageId;
            Task task = Task.Run(async () =>
            {
                byte[] expected = CreateTestData(PAGE_SIZE, (byte)capturedPageId);
                byte[] result = new byte[PAGE_SIZE];
                await cache.ReadPageAsync(capturedPageId, result);

                CollectionAssert.AreEqual(expected, result);
            });
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);
    }

    [TestMethod]
    public async Task MixedSyncAndAsyncOperations_DataConsistent()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        for (int i = 0; i < 10; i++)
        {
            byte[] data = CreateTestData(PAGE_SIZE, (byte)(i * 5));
            innerIO.WritePage(i, data);
        }

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        for (int i = 0; i < 10; i++)
        {
            byte[] expected = CreateTestData(PAGE_SIZE, (byte)(i * 5));
            byte[] syncResult = new byte[PAGE_SIZE];
            byte[] asyncResult = new byte[PAGE_SIZE];

            cache.ReadPage(i, syncResult);
            await cache.ReadPageAsync(i, asyncResult);

            CollectionAssert.AreEqual(expected, syncResult);
            CollectionAssert.AreEqual(expected, asyncResult);
        }
    }

    [TestMethod]
    public void WritePage_ThenModifyInnerIO_CacheReturnsCachedData()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        byte[] originalData = CreateTestData(PAGE_SIZE, 50);
        cache.WritePage(0, originalData);

        byte[] modifiedData = CreateTestData(PAGE_SIZE, 100);
        innerIO.WritePage(0, modifiedData);

        byte[] result = new byte[PAGE_SIZE];
        cache.ReadPage(0, result);

        CollectionAssert.AreEqual(originalData, result);
    }

    [TestMethod]
    public async Task ReadPageAsync_CacheMiss_MultipleReadersCanAccessInnerIOConcurrently()
    {
        ConcurrencyTrackingPageIO innerIO = new ConcurrencyTrackingPageIO(PAGE_SIZE, readDelayMs: 100);

        for (int i = 0; i < 10; i++)
        {
            byte[] data = CreateTestData(PAGE_SIZE, (byte)i);
            innerIO.WritePage(i, data);
        }

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 2);

        List<Task> tasks = new List<Task>();
        for (int i = 0; i < 5; i++)
        {
            int pageId = i;
            Task task = Task.Run(async () =>
            {
                byte[] result = new byte[PAGE_SIZE];
                await cache.ReadPageAsync(pageId, result);
            });
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);

        Assert.IsGreaterThan(1, innerIO.MaxConcurrentReaders, $"Expected multiple concurrent readers, but max was {innerIO.MaxConcurrentReaders}");
    }

    [TestMethod]
    public async Task ReadPageAsync_CacheHit_MultipleReadersCanAccessCacheConcurrently()
    {
        ConcurrencyTrackingPageIO innerIO = new ConcurrencyTrackingPageIO(PAGE_SIZE, readDelayMs: 50);
        byte[] testData = CreateTestData(PAGE_SIZE, 42);
        innerIO.WritePage(0, testData);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        byte[] warmup = new byte[PAGE_SIZE];
        await cache.ReadPageAsync(0, warmup);

        int concurrentCacheReads = 0;
        int maxConcurrentCacheReads = 0;
        object lockObj = new object();

        List<Task> tasks = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            Task task = Task.Run(async () =>
            {
                lock (lockObj)
                {
                    concurrentCacheReads++;
                    if (concurrentCacheReads > maxConcurrentCacheReads)
                    {
                        maxConcurrentCacheReads = concurrentCacheReads;
                    }
                }

                try
                {
                    byte[] result = new byte[PAGE_SIZE];
                    await cache.ReadPageAsync(0, result);
                    await Task.Delay(10);
                }
                finally
                {
                    lock (lockObj)
                    {
                        concurrentCacheReads--;
                    }
                }
            });
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);

        Assert.IsGreaterThan(1, maxConcurrentCacheReads, $"Expected multiple concurrent cache reads, but max was {maxConcurrentCacheReads}");
    }

    [TestMethod]
    public async Task ReadPageAsync_ConcurrentReadsAndEviction_DataNeverCorrupted()
    {
        ConcurrencyTrackingPageIO innerIO = new ConcurrencyTrackingPageIO(PAGE_SIZE, readDelayMs: 10);

        for (int i = 0; i < 20; i++)
        {
            byte[] data = CreateTestData(PAGE_SIZE, (byte)i);
            innerIO.WritePage(i, data);
        }

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 3);

        List<Task> tasks = new List<Task>();
        int errorCount = 0;

        for (int i = 0; i < 200; i++)
        {
            int pageId = i % 20;
            Task task = Task.Run(async () =>
            {
                byte[] expected = CreateTestData(PAGE_SIZE, (byte)pageId);
                byte[] result = new byte[PAGE_SIZE];
                await cache.ReadPageAsync(pageId, result);

                for (int j = 0; j < PAGE_SIZE; j++)
                {
                    if (result[j] != expected[j])
                    {
                        Interlocked.Increment(ref errorCount);
                        break;
                    }
                }
            });
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);

        Assert.AreEqual(0, errorCount, $"Data corruption detected in {errorCount} reads");
    }

    [TestMethod]
    public void WritePage_WithEviction_EvictsOldPage()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 1);

        byte[] page0Data = CreateTestData(PAGE_SIZE, 10);
        cache.WritePage(0, page0Data);

        byte[] page1Data = CreateTestData(PAGE_SIZE, 20);
        cache.WritePage(1, page1Data);

        byte[] modifiedPage0 = CreateTestData(PAGE_SIZE, 99);
        innerIO.WritePage(0, modifiedPage0);

        byte[] buffer = new byte[PAGE_SIZE];
        cache.ReadPage(0, buffer);

        CollectionAssert.AreEqual(modifiedPage0, buffer);
    }

    [TestMethod]
    public async Task ConcurrentWrites_AllDataPersisted()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 20);

        List<Task> tasks = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            int pageId = i;
            Task task = Task.Run(async () =>
            {
                byte[] data = CreateTestData(PAGE_SIZE, (byte)(pageId * 10));
                await cache.WritePageAsync(pageId, data);
            });
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);

        for (int i = 0; i < 10; i++)
        {
            byte[] expected = CreateTestData(PAGE_SIZE, (byte)(i * 10));
            byte[] result = new byte[PAGE_SIZE];
            cache.ReadPage(i, result);

            CollectionAssert.AreEqual(expected, result);
        }
    }

    [TestMethod]
    public async Task ConcurrentReadAndWriteSamePage_NoCorruption()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);
        byte[] initialData = CreateTestData(PAGE_SIZE, 50);
        innerIO.WritePage(0, initialData);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        byte[] warmup = new byte[PAGE_SIZE];
        cache.ReadPage(0, warmup);

        List<Task> tasks = new List<Task>();
        int errorCount = 0;

        for (int i = 0; i < 50; i++)
        {
            if (i % 5 == 0)
            {
                int iteration = i;
                Task writeTask = Task.Run(async () =>
                {
                    byte[] newData = CreateTestData(PAGE_SIZE, (byte)(60 + iteration));
                    await cache.WritePageAsync(0, newData);
                });
                tasks.Add(writeTask);
            }
            else
            {
                Task readTask = Task.Run(async () =>
                {
                    byte[] result = new byte[PAGE_SIZE];
                    await cache.ReadPageAsync(0, result);

                    bool valid = true;
                    byte firstByte = result[0];
                    for (int j = 0; j < PAGE_SIZE; j++)
                    {
                        byte expected = (byte)((firstByte + j) % 256);
                        if (result[j] != expected)
                        {
                            valid = false;
                            break;
                        }
                    }

                    if (!valid)
                    {
                        Interlocked.Increment(ref errorCount);
                    }
                });
                tasks.Add(readTask);
            }
        }

        await Task.WhenAll(tasks);

        Assert.AreEqual(0, errorCount, $"Data corruption detected in {errorCount} reads");
    }

    [TestMethod]
    public void WritePage_OverwriteCachedPage_ReturnsNewData()
    {
        InMemoryPageIO innerIO = new InMemoryPageIO(PAGE_SIZE);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        byte[] originalData = CreateTestData(PAGE_SIZE, 10);
        cache.WritePage(0, originalData);

        byte[] buffer = new byte[PAGE_SIZE];
        cache.ReadPage(0, buffer);
        CollectionAssert.AreEqual(originalData, buffer);

        byte[] newData = CreateTestData(PAGE_SIZE, 99);
        cache.WritePage(0, newData);

        cache.ReadPage(0, buffer);
        CollectionAssert.AreEqual(newData, buffer);
    }

    [TestMethod]
    public void Flush_PassesToInnerIO()
    {
        OperationTrackingPageIO innerIO = new OperationTrackingPageIO(PAGE_SIZE);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        cache.Flush();
        cache.Flush();

        Assert.AreEqual(2, innerIO.FlushCount);
    }

    [TestMethod]
    public async Task FlushAsync_PassesToInnerIO()
    {
        OperationTrackingPageIO innerIO = new OperationTrackingPageIO(PAGE_SIZE);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        await cache.FlushAsync();
        await cache.FlushAsync();
        await cache.FlushAsync();

        Assert.AreEqual(3, innerIO.FlushAsyncCount);
    }

    [TestMethod]
    public void SetLength_PassesToInnerIO()
    {
        OperationTrackingPageIO innerIO = new OperationTrackingPageIO(PAGE_SIZE);

        using LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        cache.SetLength(1024 * 1024);

        Assert.AreEqual(1024 * 1024, innerIO.LastSetLength);
    }

    [TestMethod]
    public void Dispose_DisposesInnerIO()
    {
        OperationTrackingPageIO innerIO = new OperationTrackingPageIO(PAGE_SIZE);

        LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        Assert.IsFalse(innerIO.IsDisposed);

        cache.Dispose();

        Assert.IsTrue(innerIO.IsDisposed);
    }

    [TestMethod]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        OperationTrackingPageIO innerIO = new OperationTrackingPageIO(PAGE_SIZE);

        LruPageCache cache = new LruPageCache(innerIO, PAGE_SIZE, 10);

        byte[] testData = CreateTestData(PAGE_SIZE, 42);
        cache.WritePage(0, testData);

        cache.Dispose();
        cache.Dispose();
        cache.Dispose();

        Assert.IsTrue(innerIO.IsDisposed);
    }

    private static byte[] CreateTestData(int size, byte seed)
    {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++)
        {
            data[i] = (byte)((seed + i) % 256);
        }
        return data;
    }
}
