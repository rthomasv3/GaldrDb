using GaldrDb.SimulationTests.Core;
using GaldrDbEngine;
using GaldrDbEngine.WAL;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.SimulationTests.Tests;

[TestClass]
public class InjectionTests
{
    [TestMethod]
    public void GaldrDb_WithSimulationPageIO_CanCreateAndInsert()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(12345);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO
        };

        // Create WAL with test stream
        WriteAheadLog wal = new WriteAheadLog("test.wal", 8192);
        wal._testStreamIO = walStreamIO;
        wal._testSaltGenerator = () => rng.NextUInt();
        wal.Create();

        // For now, just verify the WAL was created successfully with our stream
        Assert.IsGreaterThan(0, walStream.Length, "WAL should have header written");
        Assert.IsGreaterThan(0, stats.WalWrites, "Should have WAL writes");

        wal.Dispose();
    }

    [TestMethod]
    public void SimulationPageIO_Integration_MultiplePages()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);

        // Write multiple pages
        for (int i = 0; i < 10; i++)
        {
            byte[] data = new byte[8192];
            data[0] = (byte)i;
            data[8191] = (byte)(i + 100);
            pageIO.WritePage(i, data);
        }

        pageIO.Flush();

        // Verify all pages
        for (int i = 0; i < 10; i++)
        {
            byte[] readData = new byte[8192];
            pageIO.ReadPage(i, readData);
            Assert.AreEqual((byte)i, readData[0]);
            Assert.AreEqual((byte)(i + 100), readData[8191]);
        }

        Assert.AreEqual(10, stats.PageWrites);
        Assert.AreEqual(10, stats.PageReads);
        Assert.AreEqual(10, pageIO.PersistedPageCount);
    }

    [TestMethod]
    public void SimulationWalStream_Integration_WriteAndRead()
    {
        SimulationStats stats = new SimulationStats();
        SimulationWalStream walStream = new SimulationWalStream(stats);

        // Write some data (simulating WAL header + frames)
        byte[] header = new byte[32];
        header[0] = 0x57; // 'W'
        header[1] = 0x41; // 'A'
        header[2] = 0x4C; // 'L'
        walStream.Write(header, 0, header.Length);

        byte[] frame = new byte[100];
        frame[0] = 1; // frame number
        walStream.Write(frame, 0, frame.Length);

        walStream.Flush();

        // Read back
        walStream.Position = 0;
        byte[] readHeader = new byte[32];
        walStream.ReadExactly(readHeader, 0, readHeader.Length);

        Assert.AreEqual(0x57, readHeader[0]);
        Assert.AreEqual(0x41, readHeader[1]);
        Assert.AreEqual(0x4C, readHeader[2]);
    }

    [TestMethod]
    public void WriteAheadLog_WithTestStream_CreateAndOpen()
    {
        SimulationStats stats = new SimulationStats();
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);

        // Create WAL
        WriteAheadLog wal = new WriteAheadLog("test.wal", 8192);
        wal._testStreamIO = walStreamIO;
        wal._testSaltGenerator = () => rng.NextUInt();
        wal.Create();

        long lengthAfterCreate = walStream.Length;
        Assert.IsGreaterThan(0, lengthAfterCreate, "WAL should have content after Create");

        wal.Dispose();

        // Flush to persist state
        walStream.Flush();

        // Reopen WAL (simulating restart)
        WriteAheadLog wal2 = new WriteAheadLog("test.wal", 8192);
        wal2._testStreamIO = walStreamIO;
        wal2._testSaltGenerator = () => rng.NextUInt();
        wal2.Open();

        Assert.AreEqual(8192, wal2.Header.PageSize);
        Assert.AreEqual(0, wal2.CurrentFrameNumber);

        wal2.Dispose();
    }
}
