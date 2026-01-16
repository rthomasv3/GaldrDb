using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDb.SimulationTests.Core;
using GaldrDb.SimulationTests.Workload;

namespace GaldrDb.SimulationTests.Tests;

[TestClass]
public class CoreInfrastructureTests
{
    [TestMethod]
    public void SimulationRandom_SameSeed_ProducesSameSequence()
    {
        SimulationRandom rng1 = new SimulationRandom(12345);
        SimulationRandom rng2 = new SimulationRandom(12345);

        for (int i = 0; i < 100; i++)
        {
            Assert.AreEqual(rng1.Next(), rng2.Next());
        }
    }

    [TestMethod]
    public void SimulationRandom_DifferentSeeds_ProduceDifferentSequences()
    {
        SimulationRandom rng1 = new SimulationRandom(12345);
        SimulationRandom rng2 = new SimulationRandom(54321);

        bool allSame = true;
        for (int i = 0; i < 100; i++)
        {
            if (rng1.Next() != rng2.Next())
            {
                allSame = false;
                break;
            }
        }

        Assert.IsFalse(allSame);
    }

    [TestMethod]
    public void SimulationPageIO_WriteAndRead_ReturnsWrittenData()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(4096, stats);

        byte[] writeData = new byte[4096];
        for (int i = 0; i < writeData.Length; i++)
        {
            writeData[i] = (byte)(i % 256);
        }

        pageIO.WritePage(0, writeData);

        byte[] readData = new byte[4096];
        pageIO.ReadPage(0, readData);

        CollectionAssert.AreEqual(writeData, readData);
        Assert.AreEqual(1, stats.PageWrites);
        Assert.AreEqual(1, stats.PageReads);
    }

    [TestMethod]
    public void SimulationPageIO_FlushPersistsData()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(4096, stats);

        byte[] writeData = new byte[4096];
        writeData[0] = 42;

        pageIO.WritePage(0, writeData);
        Assert.AreEqual(0, pageIO.PersistedPageCount);
        Assert.AreEqual(1, pageIO.UnflushedPageCount);

        pageIO.Flush();
        Assert.AreEqual(1, pageIO.PersistedPageCount);
        Assert.AreEqual(0, pageIO.UnflushedPageCount);
        Assert.AreEqual(1, stats.PageFlushes);
    }

    [TestMethod]
    public void SimulationPageIO_CrashDiscardsUnflushedData()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(4096, stats);

        byte[] data1 = new byte[4096];
        data1[0] = 1;
        pageIO.WritePage(0, data1);
        pageIO.Flush();

        byte[] data2 = new byte[4096];
        data2[0] = 2;
        pageIO.WritePage(0, data2);

        // Before crash, should read data2
        byte[] readBefore = new byte[4096];
        pageIO.ReadPage(0, readBefore);
        Assert.AreEqual(2, readBefore[0]);

        // Simulate crash
        pageIO.SimulateCrash();

        // After crash, should read data1 (persisted version)
        byte[] readAfter = new byte[4096];
        pageIO.ReadPage(0, readAfter);
        Assert.AreEqual(1, readAfter[0]);

        Assert.AreEqual(1, stats.CrashCount);
    }

    [TestMethod]
    public void SimulationWalStream_FlushPersistsData()
    {
        SimulationStats stats = new SimulationStats();
        SimulationWalStream walStream = new SimulationWalStream(stats);

        byte[] data = new byte[] { 1, 2, 3, 4, 5 };
        walStream.Write(data, 0, data.Length);

        Assert.IsTrue(walStream.HasUnflushedWrites);
        Assert.AreEqual(0, walStream.PersistedLength);

        walStream.Flush();

        Assert.IsFalse(walStream.HasUnflushedWrites);
        Assert.AreEqual(5, walStream.PersistedLength);
    }

    [TestMethod]
    public void SimulationWalStream_CrashDiscardsUnflushedData()
    {
        SimulationStats stats = new SimulationStats();
        SimulationWalStream walStream = new SimulationWalStream(stats);

        byte[] data1 = new byte[] { 1, 2, 3 };
        walStream.Write(data1, 0, data1.Length);
        walStream.Flush();

        byte[] data2 = new byte[] { 4, 5, 6, 7, 8 };
        walStream.Write(data2, 0, data2.Length);

        Assert.AreEqual(8, walStream.Length); // 3 + 5

        walStream.SimulateCrash();

        Assert.AreEqual(3, walStream.Length); // Back to persisted state
        // Note: CrashCount is incremented by SimulationPageIO, not WalStream
    }

    [TestMethod]
    public void TestDocument_Generate_CreatesValidDocument()
    {
        SimulationRandom rng = new SimulationRandom(42);
        TestDocument doc = TestDocument.Generate(rng, 500);

        Assert.IsNotNull(doc.Name);
        Assert.IsGreaterThanOrEqualTo(10, doc.Name.Length);
        Assert.IsNotNull(doc.Category);
        Assert.IsNotNull(doc.Payload);
    }

    [TestMethod]
    public void TestDocument_ComputeHash_IsDeterministic()
    {
        SimulationRandom rng = new SimulationRandom(42);
        TestDocument doc = TestDocument.Generate(rng, 500);

        byte[] hash1 = doc.ComputeHash();
        byte[] hash2 = doc.ComputeHash();

        CollectionAssert.AreEqual(hash1, hash2);
    }

    [TestMethod]
    public void TestDocument_DifferentDocuments_HaveDifferentHashes()
    {
        SimulationRandom rng = new SimulationRandom(42);
        TestDocument doc1 = TestDocument.Generate(rng, 500);
        TestDocument doc2 = TestDocument.Generate(rng, 500);

        byte[] hash1 = doc1.ComputeHash();
        byte[] hash2 = doc2.ComputeHash();

        CollectionAssert.AreNotEqual(hash1, hash2);
    }
}
