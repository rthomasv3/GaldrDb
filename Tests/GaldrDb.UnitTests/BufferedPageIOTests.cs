using System;
using System.Collections.Generic;
using GaldrDbEngine.IO;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class BufferedPageIOTests
{
    private const int PageSize = 4096;

    private InMemoryPageIO _innerIO;
    private BufferedPageIO _bufferedIO;

    [TestInitialize]
    public void Setup()
    {
        _innerIO = new InMemoryPageIO(PageSize);
        _bufferedIO = new BufferedPageIO(_innerIO, PageSize);
    }

    [TestCleanup]
    public void Cleanup()
    {
        _bufferedIO?.Dispose();
    }

    [TestMethod]
    public void ReadYourOwnWrites_ReturnsBufferedData()
    {
        TransactionContext ctx = _bufferedIO.BeginSnapshot(1, 1, 0);
        _bufferedIO.BeginWrite(ctx);

        byte[] writeData = new byte[PageSize];
        writeData[0] = 0xAB;
        writeData[1] = 0xCD;
        _bufferedIO.WritePage(0, writeData, ctx);

        byte[] readData = new byte[PageSize];
        _bufferedIO.ReadPage(0, readData, ctx);

        Assert.AreEqual(0xAB, readData[0]);
        Assert.AreEqual(0xCD, readData[1]);

        _bufferedIO.AbortWrite(ctx);
        _bufferedIO.EndSnapshot(ctx);
    }

    [TestMethod]
    public void CommitWrite_FlushesToInnerIO()
    {
        TransactionContext ctx = _bufferedIO.BeginSnapshot(1, 1, 0);
        _bufferedIO.BeginWrite(ctx);

        byte[] writeData = new byte[PageSize];
        writeData[0] = 0x42;
        _bufferedIO.WritePage(5, writeData, ctx);

        // Before commit, inner IO should not have the data
        byte[] innerRead = new byte[PageSize];
        _innerIO.ReadPage(5, innerRead);
        Assert.AreEqual(0, innerRead[0]);

        _bufferedIO.CommitWrite(ctx);

        // After commit, inner IO should have the data
        _innerIO.ReadPage(5, innerRead);
        Assert.AreEqual(0x42, innerRead[0]);

        _bufferedIO.EndSnapshot(ctx);
    }

    [TestMethod]
    public void AbortWrite_DiscardsBuffers()
    {
        TransactionContext ctx = _bufferedIO.BeginSnapshot(1, 1, 0);
        _bufferedIO.BeginWrite(ctx);

        byte[] writeData = new byte[PageSize];
        writeData[0] = 0xFF;
        _bufferedIO.WritePage(3, writeData, ctx);

        _bufferedIO.AbortWrite(ctx);

        // After abort, inner IO should not have the data
        byte[] innerRead = new byte[PageSize];
        _innerIO.ReadPage(3, innerRead);
        Assert.AreEqual(0, innerRead[0]);

        // Reading with no context should return empty
        byte[] readData = new byte[PageSize];
        _bufferedIO.ReadPage(3, readData);
        Assert.AreEqual(0, readData[0]);

        _bufferedIO.EndSnapshot(ctx);
    }

    [TestMethod]
    public void ConflictDetection_SamePage_ThrowsPageConflictException()
    {
        // Tx1 starts and writes page 10
        TransactionContext ctx1 = _bufferedIO.BeginSnapshot(1, 1, 0);
        _bufferedIO.BeginWrite(ctx1);

        byte[] data1 = new byte[PageSize];
        data1[0] = 0x01;
        _bufferedIO.WritePage(10, data1, ctx1);

        // Tx2 starts (same snapshot), writes and commits page 10
        TransactionContext ctx2 = _bufferedIO.BeginSnapshot(2, 2, 0);
        _bufferedIO.BeginWrite(ctx2);

        byte[] data2 = new byte[PageSize];
        data2[0] = 0x02;
        _bufferedIO.WritePage(10, data2, ctx2);
        _bufferedIO.CommitWrite(ctx2);
        _bufferedIO.EndSnapshot(ctx2);

        // Tx1 tries to commit — should conflict
        Assert.ThrowsExactly<PageConflictException>(() =>
        {
            _bufferedIO.CommitWrite(ctx1);
        });

        _bufferedIO.AbortWrite(ctx1);
        _bufferedIO.EndSnapshot(ctx1);
    }

    [TestMethod]
    public void NoConflict_DifferentPages_BothSucceed()
    {
        // Tx1 writes page 10
        TransactionContext ctx1 = _bufferedIO.BeginSnapshot(1, 1, 0);
        _bufferedIO.BeginWrite(ctx1);
        byte[] data1 = new byte[PageSize];
        data1[0] = 0x01;
        _bufferedIO.WritePage(10, data1, ctx1);

        // Tx2 writes page 20 and commits
        TransactionContext ctx2 = _bufferedIO.BeginSnapshot(2, 2, 0);
        _bufferedIO.BeginWrite(ctx2);
        byte[] data2 = new byte[PageSize];
        data2[0] = 0x02;
        _bufferedIO.WritePage(20, data2, ctx2);
        _bufferedIO.CommitWrite(ctx2);
        _bufferedIO.EndSnapshot(ctx2);

        // Tx1 should commit successfully (different page)
        _bufferedIO.CommitWrite(ctx1);
        _bufferedIO.EndSnapshot(ctx1);

        // Verify both writes persisted
        byte[] read1 = new byte[PageSize];
        byte[] read2 = new byte[PageSize];
        _innerIO.ReadPage(10, read1);
        _innerIO.ReadPage(20, read2);
        Assert.AreEqual(0x01, read1[0]);
        Assert.AreEqual(0x02, read2[0]);
    }

    [TestMethod]
    public void RefreshSnapshot_AllowsRetryAfterConflict()
    {
        // Tx1 writes page 10
        TransactionContext ctx1 = _bufferedIO.BeginSnapshot(1, 1, 0);
        _bufferedIO.BeginWrite(ctx1);
        byte[] data1 = new byte[PageSize];
        data1[0] = 0x01;
        _bufferedIO.WritePage(10, data1, ctx1);

        // Tx2 writes and commits page 10
        TransactionContext ctx2 = _bufferedIO.BeginSnapshot(2, 2, 0);
        _bufferedIO.BeginWrite(ctx2);
        byte[] data2 = new byte[PageSize];
        data2[0] = 0x02;
        _bufferedIO.WritePage(10, data2, ctx2);
        _bufferedIO.CommitWrite(ctx2);
        _bufferedIO.EndSnapshot(ctx2);

        // Tx1 conflicts
        Assert.ThrowsExactly<PageConflictException>(() =>
        {
            _bufferedIO.CommitWrite(ctx1);
        });

        // Abort and refresh snapshot
        _bufferedIO.AbortWrite(ctx1);
        _bufferedIO.RefreshSnapshot(ctx1);

        // Retry with fresh snapshot
        _bufferedIO.BeginWrite(ctx1);
        byte[] data3 = new byte[PageSize];
        data3[0] = 0x03;
        _bufferedIO.WritePage(10, data3, ctx1);
        _bufferedIO.CommitWrite(ctx1); // Should succeed now
        _bufferedIO.EndSnapshot(ctx1);

        // Verify final value
        byte[] readData = new byte[PageSize];
        _innerIO.ReadPage(10, readData);
        Assert.AreEqual(0x03, readData[0]);
    }

    [TestMethod]
    public void NullContext_WriteThrough()
    {
        byte[] writeData = new byte[PageSize];
        writeData[0] = 0x99;
        _bufferedIO.WritePage(7, writeData);

        // Should be immediately visible in inner IO
        byte[] readData = new byte[PageSize];
        _innerIO.ReadPage(7, readData);
        Assert.AreEqual(0x99, readData[0]);
    }

    [TestMethod]
    public void Checkpoint_ReturnsFalse()
    {
        Assert.IsFalse(_bufferedIO.Checkpoint());
    }

    [TestMethod]
    public void MultipleWritesSamePage_KeepsLatestInBuffer()
    {
        TransactionContext ctx = _bufferedIO.BeginSnapshot(1, 1, 0);
        _bufferedIO.BeginWrite(ctx);

        byte[] data1 = new byte[PageSize];
        data1[0] = 0x01;
        _bufferedIO.WritePage(5, data1, ctx);

        byte[] data2 = new byte[PageSize];
        data2[0] = 0x02;
        _bufferedIO.WritePage(5, data2, ctx);

        // Read should return the latest write
        byte[] readData = new byte[PageSize];
        _bufferedIO.ReadPage(5, readData, ctx);
        Assert.AreEqual(0x02, readData[0]);

        _bufferedIO.CommitWrite(ctx);
        _bufferedIO.EndSnapshot(ctx);

        // Inner IO should have the latest write
        byte[] innerRead = new byte[PageSize];
        _innerIO.ReadPage(5, innerRead);
        Assert.AreEqual(0x02, innerRead[0]);
    }

    [TestMethod]
    public void ReadFallsBackToInnerIO_WhenNotInBuffer()
    {
        // Write directly to inner IO
        byte[] innerData = new byte[PageSize];
        innerData[0] = 0x77;
        _innerIO.WritePage(8, innerData);

        // Read through BufferedPageIO with a transaction context
        TransactionContext ctx = _bufferedIO.BeginSnapshot(1, 1, 0);
        _bufferedIO.BeginWrite(ctx);

        byte[] readData = new byte[PageSize];
        _bufferedIO.ReadPage(8, readData, ctx);
        Assert.AreEqual(0x77, readData[0]);

        _bufferedIO.AbortWrite(ctx);
        _bufferedIO.EndSnapshot(ctx);
    }

    [TestMethod]
    public void CommitWrite_NullContext_Throws()
    {
        Assert.ThrowsExactly<InvalidOperationException>(() =>
        {
            _bufferedIO.CommitWrite(null);
        });
    }
}
