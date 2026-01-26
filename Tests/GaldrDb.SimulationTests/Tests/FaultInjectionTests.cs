using System;
using System.IO;
using GaldrDb.SimulationTests.Core;
using GaldrDb.SimulationTests.Workload;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.SimulationTests.Tests;

[TestClass]
public class FaultInjectionTests
{
    [TestMethod]
    public void FaultInjector_ReadError_ThrowsIOException()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationRandom rng = new SimulationRandom(42);
        FaultInjector faultInjector = new FaultInjector(rng, stats);

        pageIO.SetFaultInjector(faultInjector);

        // Write a page first
        byte[] data = new byte[8192];
        data[0] = 0x42;
        pageIO.WritePage(0, data);
        pageIO.Flush();

        // Schedule a read error
        faultInjector.Enabled = true;
        faultInjector.ScheduleFaultAfter(0, FaultType.ReadError);

        // Read should throw
        byte[] readBuffer = new byte[8192];
        Assert.Throws<IOException>(() => pageIO.ReadPage(0, readBuffer));

        Assert.AreEqual(1, stats.ReadErrorsInjected);
    }

    [TestMethod]
    public void FaultInjector_WriteError_ThrowsIOException()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationRandom rng = new SimulationRandom(42);
        FaultInjector faultInjector = new FaultInjector(rng, stats);

        pageIO.SetFaultInjector(faultInjector);

        // Schedule a write error
        faultInjector.Enabled = true;
        faultInjector.ScheduleFaultAfter(0, FaultType.WriteError);

        // Write should throw
        byte[] data = new byte[8192];
        Assert.Throws<IOException>(() => pageIO.WritePage(0, data));

        Assert.AreEqual(1, stats.WriteErrorsInjected);
    }

    [TestMethod]
    public void FaultInjector_CorruptRead_ModifiesData()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationRandom rng = new SimulationRandom(42);
        FaultInjector faultInjector = new FaultInjector(rng, stats);

        pageIO.SetFaultInjector(faultInjector);

        // Write known data
        byte[] originalData = new byte[8192];
        for (int i = 0; i < originalData.Length; i++)
        {
            originalData[i] = (byte)(i % 256);
        }
        pageIO.WritePage(0, originalData);
        pageIO.Flush();

        // Schedule a corrupt read
        faultInjector.Enabled = true;
        faultInjector.ScheduleFaultAfter(0, FaultType.CorruptRead);

        // Read - data should be corrupted
        byte[] readBuffer = new byte[8192];
        pageIO.ReadPage(0, readBuffer);

        // Verify data is different (corrupted)
        bool foundDifference = false;
        for (int i = 0; i < readBuffer.Length; i++)
        {
            if (readBuffer[i] != originalData[i])
            {
                foundDifference = true;
                break;
            }
        }

        Assert.IsTrue(foundDifference, "Corrupted read should have modified data");
        Assert.AreEqual(1, stats.CorruptReadsInjected);
    }

    [TestMethod]
    public void FaultInjector_PartialWrite_WritesIncompleteData()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationRandom rng = new SimulationRandom(42);
        FaultInjector faultInjector = new FaultInjector(rng, stats);

        pageIO.SetFaultInjector(faultInjector);

        // Schedule a partial write
        faultInjector.Enabled = true;
        faultInjector.ScheduleFaultAfter(0, FaultType.PartialWrite);

        // Write full page of 0xFF
        byte[] data = new byte[8192];
        Array.Fill(data, (byte)0xFF);
        pageIO.WritePage(0, data);
        pageIO.Flush();

        // Read back - should have partial data (some zeros at the end)
        byte[] readBuffer = new byte[8192];
        pageIO.ReadPage(0, readBuffer);

        // Find where the zeros start
        int lastNonZero = -1;
        for (int i = readBuffer.Length - 1; i >= 0; i--)
        {
            if (readBuffer[i] != 0)
            {
                lastNonZero = i;
                break;
            }
        }

        Assert.IsLessThan(readBuffer.Length - 1, lastNonZero, "Partial write should leave trailing zeros");
        Assert.AreEqual(1, stats.PartialWritesInjected);
    }

    [TestMethod]
    public void FaultInjector_ProbabilisticFaults_InjectMultipleFaults()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationRandom rng = new SimulationRandom(42);
        FaultInjector faultInjector = new FaultInjector(rng, stats);

        pageIO.SetFaultInjector(faultInjector);

        // Set high probability for corrupt reads
        faultInjector.Enabled = true;
        faultInjector.SetCorruptReadProbability(0.5);

        // Write some pages
        for (int i = 0; i < 10; i++)
        {
            byte[] data = new byte[8192];
            data[0] = (byte)i;
            pageIO.WritePage(i, data);
        }
        pageIO.Flush();

        // Read many times - should get some corruptions
        byte[] readBuffer = new byte[8192];
        int readAttempts = 100;

        for (int i = 0; i < readAttempts; i++)
        {
            try
            {
                pageIO.ReadPage(i % 10, readBuffer);
            }
            catch (IOException)
            {
                // Read errors might also happen
            }
        }

        // With 50% probability, we should have gotten some corruptions
        Assert.IsGreaterThan(0, stats.CorruptReadsInjected, "Should have injected at least one corrupt read");
    }

    [TestMethod]
    public void Database_WithReadError_HandlesGracefully()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);
        FaultInjector faultInjector = new FaultInjector(rng, stats);

        pageIO.SetFaultInjector(faultInjector);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            PageCacheSize = 0,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        // Create database and insert document
        int insertedId;
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("fault_test.db", options))
        {
            pageIO.Flush(); // Ensure initial setup is durable

            using (Transaction tx = db.BeginTransaction())
            {
                TestDocument doc = TestDocument.Generate(rng, 200);
                insertedId = tx.Insert(doc);
                tx.Commit();
            }
        }

        // Reset disposed state to allow reopening (Close() already flushed)
        pageIO.ResetDisposed();

        // Reopen database
        options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            PageCacheSize = 0,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Open("fault_test.db", options))
        {
            // Enable fault injection - occasional read errors
            faultInjector.Enabled = true;
            faultInjector.SetReadErrorProbability(0.1);

            // Try to read multiple times - some should fail, some should succeed
            int successCount = 0;
            int failureCount = 0;

            for (int i = 0; i < 20; i++)
            {
                try
                {
                    using (Transaction tx = db.BeginReadOnlyTransaction())
                    {
                        TestDocument doc = tx.GetById<TestDocument>(insertedId);
                        if (doc != null)
                        {
                            successCount++;
                        }
                    }
                }
                catch (IOException)
                {
                    failureCount++;
                }
            }

            // Should have some successes and some failures
            Assert.IsGreaterThan(0, successCount, "Should have some successful reads");
            Assert.IsGreaterThan(0, failureCount, "Should have some failed reads");
        }
    }

    [TestMethod]
    public void Database_WithWriteError_TransactionFails()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);
        FaultInjector faultInjector = new FaultInjector(rng, stats);

        pageIO.SetFaultInjector(faultInjector);
        walStream.SetFaultInjector(faultInjector);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("fault_test2.db", options))
        {
            pageIO.Flush();

            // Schedule a write error after some operations
            faultInjector.Enabled = true;
            faultInjector.ScheduleFaultAfter(5, FaultType.WriteError);

            // Try to insert - should eventually fail
            bool gotWriteError = false;
            try
            {
                for (int i = 0; i < 10; i++)
                {
                    using (Transaction tx = db.BeginTransaction())
                    {
                        TestDocument doc = TestDocument.Generate(rng, 200);
                        tx.Insert(doc);
                        tx.Commit();
                    }
                }
            }
            catch (IOException)
            {
                gotWriteError = true;
            }

            Assert.IsTrue(gotWriteError, "Should have gotten a write error");
            Assert.AreEqual(1, stats.WriteErrorsInjected);
        }
    }

    [TestMethod]
    public void FaultInjector_Disabled_NoFaultsInjected()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationRandom rng = new SimulationRandom(42);
        FaultInjector faultInjector = new FaultInjector(rng, stats);

        pageIO.SetFaultInjector(faultInjector);

        // Configure faults but don't enable
        faultInjector.SetReadErrorProbability(1.0); // 100% probability
        faultInjector.SetWriteErrorProbability(1.0);
        // faultInjector.Enabled = false; // Default is false

        // Write and read should work without faults
        byte[] data = new byte[8192];
        data[0] = 0x42;
        pageIO.WritePage(0, data);
        pageIO.Flush();

        byte[] readBuffer = new byte[8192];
        pageIO.ReadPage(0, readBuffer);

        Assert.AreEqual(0x42, readBuffer[0]);
        Assert.AreEqual(0, stats.FaultsInjected);
    }
}
