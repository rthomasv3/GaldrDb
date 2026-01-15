using System;
using System.Collections.Generic;
using System.IO;
using GaldrDbEngine.WAL;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class WalTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbWalTests_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    #region WalHeader Tests

    [TestMethod]
    public void WalHeader_Serialize_RoundTrip_Success()
    {
        WalHeader header = new WalHeader
        {
            PageSize = 8192,
            CheckpointTxId = 12345,
            Salt1 = 42,
            Salt2 = 999
        };

        byte[] buffer = new byte[WalHeader.HEADER_SIZE];
        header.SerializeTo(buffer);
        WalHeader deserialized = WalHeader.Deserialize(buffer);

        Assert.AreEqual(WalHeader.WAL_MAGIC_NUMBER, deserialized.MagicNumber);
        Assert.AreEqual(1, deserialized.Version);
        Assert.AreEqual(8192, deserialized.PageSize);
        Assert.AreEqual(12345ul, deserialized.CheckpointTxId);
        Assert.AreEqual(42u, deserialized.Salt1);
        Assert.AreEqual(999u, deserialized.Salt2);
    }

    [TestMethod]
    public void WalHeader_ValidateChecksum_ValidHeader_ReturnsTrue()
    {
        WalHeader header = new WalHeader
        {
            PageSize = 4096,
            CheckpointTxId = 999,
            Salt1 = 1,
            Salt2 = 12345
        };

        byte[] buffer = new byte[WalHeader.HEADER_SIZE];
        header.SerializeTo(buffer);
        WalHeader deserialized = WalHeader.Deserialize(buffer);

        bool result = deserialized.ValidateChecksum();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void WalHeader_ValidateMagicNumber_ValidHeader_ReturnsTrue()
    {
        WalHeader header = new WalHeader();

        bool result = header.ValidateMagicNumber();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void WalHeader_ValidateMagicNumber_InvalidMagic_ReturnsFalse()
    {
        WalHeader header = new WalHeader();
        header.MagicNumber = 0x12345678;

        bool result = header.ValidateMagicNumber();

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void WalHeader_HeaderSize_Is32Bytes()
    {
#pragma warning disable MSTEST0032
        Assert.AreEqual(32, WalHeader.HEADER_SIZE);
#pragma warning restore MSTEST0032
    }

    #endregion

    #region WalFrame Tests

    [TestMethod]
    public void WalFrame_Serialize_RoundTrip_Success()
    {
        byte[] testData = new byte[100];
        for (int i = 0; i < testData.Length; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        WalFrame frame = new WalFrame
        {
            FrameNumber = 42,
            TxId = 12345,
            PageId = 10,
            PageType = 0x01,
            Flags = WalFrameFlags.Commit,
            Data = testData
        };

        byte[] serialized = new byte[WalFrame.FRAME_HEADER_SIZE + testData.Length];
        frame.SerializeTo(serialized);
        WalFrame deserialized = WalFrame.Deserialize(serialized);

        Assert.AreEqual(42, deserialized.FrameNumber);
        Assert.AreEqual(12345ul, deserialized.TxId);
        Assert.AreEqual(10, deserialized.PageId);
        Assert.AreEqual(0x01, deserialized.PageType);
        Assert.AreEqual(WalFrameFlags.Commit, deserialized.Flags);
        CollectionAssert.AreEqual(testData, deserialized.Data);
    }

    [TestMethod]
    public void WalFrame_ValidateChecksum_ValidFrame_ReturnsTrue()
    {
        byte[] testData = new byte[] { 1, 2, 3, 4, 5 };
        int pageSize = 8192;

        WalFrame frame = new WalFrame
        {
            FrameNumber = 1,
            TxId = 100,
            PageId = 5,
            PageType = 0x02,
            Flags = WalFrameFlags.None,
            Data = testData
        };

        byte[] serialized = new byte[WalFrame.FRAME_HEADER_SIZE + pageSize];
        frame.SerializeTo(serialized);
        WalFrame deserialized = WalFrame.Deserialize(serialized);

        bool result = deserialized.ValidateChecksum(pageSize);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void WalFrame_IsCommit_CommitFlagSet_ReturnsTrue()
    {
        WalFrame frame = new WalFrame
        {
            Flags = WalFrameFlags.Commit
        };

        bool result = frame.IsCommit();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void WalFrame_IsCommit_NoCommitFlag_ReturnsFalse()
    {
        WalFrame frame = new WalFrame
        {
            Flags = WalFrameFlags.None
        };

        bool result = frame.IsCommit();

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void WalFrame_IsCheckpoint_CheckpointFlagSet_ReturnsTrue()
    {
        WalFrame frame = new WalFrame
        {
            Flags = WalFrameFlags.Checkpoint
        };

        bool result = frame.IsCheckpoint();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void WalFrame_EmptyData_SerializesCorrectly()
    {
        int pageSize = 8192;

        WalFrame frame = new WalFrame
        {
            FrameNumber = 1,
            TxId = 1,
            PageId = 0,
            PageType = 0x01,
            Flags = WalFrameFlags.None,
            Data = Array.Empty<byte>()
        };

        byte[] serialized = new byte[WalFrame.FRAME_HEADER_SIZE + pageSize];
        frame.SerializeTo(serialized);
        WalFrame deserialized = WalFrame.Deserialize(serialized);

        Assert.IsEmpty(deserialized.Data);
        Assert.IsTrue(deserialized.ValidateChecksum(pageSize));
    }

    [TestMethod]
    public void WalFrame_FrameHeaderSize_Is40Bytes()
    {
#pragma warning disable MSTEST0032
        Assert.AreEqual(40, WalFrame.FRAME_HEADER_SIZE);
#pragma warning restore MSTEST0032
    }

    #endregion

    #region WriteAheadLog Tests

    [TestMethod]
    public void WriteAheadLog_Create_CreatesFile()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Create();
        }

        Assert.IsTrue(File.Exists(walPath));
    }

    [TestMethod]
    public void WriteAheadLog_Create_InitializesHeader()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Create();

            Assert.AreEqual(8192, wal.Header.PageSize);
            Assert.AreEqual(0ul, wal.Header.CheckpointTxId);
            Assert.AreEqual(1u, wal.Header.Salt1);
            Assert.AreNotEqual(0u, wal.Header.Salt2);
            Assert.AreEqual(0, wal.CurrentFrameNumber);
        }
    }

    [TestMethod]
    public void WriteAheadLog_Create_FileAlreadyExists_ThrowsException()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        File.WriteAllText(walPath, "existing");

        bool exceptionThrown = false;

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            try
            {
                wal.Create();
            }
            catch (InvalidOperationException)
            {
                exceptionThrown = true;
            }
        }

        Assert.IsTrue(exceptionThrown);
    }

    [TestMethod]
    public void WriteAheadLog_Open_ReadsHeader()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Create();
        }

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Open();

            Assert.AreEqual(8192, wal.Header.PageSize);
            Assert.AreEqual(0ul, wal.Header.CheckpointTxId);
        }
    }

    [TestMethod]
    public void WriteAheadLog_Open_FileNotFound_ThrowsException()
    {
        string walPath = Path.Combine(_testDirectory, "nonexistent.wal");

        bool exceptionThrown = false;

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            try
            {
                wal.Open();
            }
            catch (FileNotFoundException)
            {
                exceptionThrown = true;
            }
        }

        Assert.IsTrue(exceptionThrown);
    }

    [TestMethod]
    public void WriteAheadLog_Open_PageSizeMismatch_ThrowsException()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Create();
        }

        bool exceptionThrown = false;

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 4096))
        {
            try
            {
                wal.Open();
            }
            catch (InvalidOperationException)
            {
                exceptionThrown = true;
            }
        }

        Assert.IsTrue(exceptionThrown);
    }

    [TestMethod]
    public void WriteAheadLog_WriteFrame_IncrementsFrameNumber()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        byte[] testData = new byte[8192];

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Create();

            long frame1 = wal.WriteFrame(1, 0, 0x01, testData, WalFrameFlags.None);
            long frame2 = wal.WriteFrame(1, 1, 0x01, testData, WalFrameFlags.Commit);

            Assert.AreEqual(1, frame1);
            Assert.AreEqual(2, frame2);
            Assert.AreEqual(2, wal.CurrentFrameNumber);
        }
    }

    [TestMethod]
    public void WriteAheadLog_WriteFrame_PersistsAcrossReopen()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        byte[] testData = new byte[8192];
        for (int i = 0; i < testData.Length; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Create();
            wal.WriteFrame(100, 5, 0x01, testData, WalFrameFlags.Commit);
            wal.Flush();
        }

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Open();

            List<WalFrame> frames = wal.ReadAllFrames();

            Assert.HasCount(1, frames);
            Assert.AreEqual(100ul, frames[0].TxId);
            Assert.AreEqual(5, frames[0].PageId);
            Assert.AreEqual(0x01, frames[0].PageType);
            Assert.AreEqual(WalFrameFlags.Commit, frames[0].Flags);
            CollectionAssert.AreEqual(testData, frames[0].Data);
        }
    }

    [TestMethod]
    public void WriteAheadLog_ReadAllFrames_EmptyWal_ReturnsEmptyList()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Create();

            List<WalFrame> frames = wal.ReadAllFrames();

            Assert.IsEmpty(frames);
        }
    }

    [TestMethod]
    public void WriteAheadLog_ReadAllFrames_MultipleFrames_ReturnsAll()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        int pageSize = 4096;

        using (WriteAheadLog wal = new WriteAheadLog(walPath, pageSize))
        {
            wal.Create();

            for (int i = 0; i < 10; i++)
            {
                byte[] data = new byte[pageSize];
                data[0] = (byte)i;
                WalFrameFlags flags = (i == 9) ? WalFrameFlags.Commit : WalFrameFlags.None;
                wal.WriteFrame(1, i, 0x01, data, flags);
            }

            List<WalFrame> frames = wal.ReadAllFrames();

            Assert.HasCount(10, frames);
            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(i + 1, frames[i].FrameNumber);
                Assert.AreEqual(i, frames[i].PageId);
                Assert.AreEqual((byte)i, frames[i].Data[0]);
            }
        }
    }

    [TestMethod]
    public void WriteAheadLog_ReadFramesFromPosition_ReturnsSubset()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        int pageSize = 4096;

        using (WriteAheadLog wal = new WriteAheadLog(walPath, pageSize))
        {
            wal.Create();

            for (int i = 0; i < 10; i++)
            {
                byte[] data = new byte[pageSize];
                wal.WriteFrame(1, i, 0x01, data, WalFrameFlags.None);
            }

            List<WalFrame> frames = wal.ReadFramesFromPosition(5);

            Assert.HasCount(6, frames);
            Assert.AreEqual(5, frames[0].FrameNumber);
            Assert.AreEqual(10, frames[5].FrameNumber);
        }
    }

    [TestMethod]
    public void WriteAheadLog_Truncate_ClearsAllFrames()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        byte[] testData = new byte[4096];

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 4096))
        {
            wal.Create();
            uint originalSalt1 = wal.Header.Salt1;
            uint originalSalt2 = wal.Header.Salt2;

            wal.WriteFrame(1, 0, 0x01, testData, WalFrameFlags.None);
            wal.WriteFrame(1, 1, 0x01, testData, WalFrameFlags.Commit);

            Assert.AreEqual(2, wal.CurrentFrameNumber);

            wal.Truncate();

            Assert.AreEqual(0, wal.CurrentFrameNumber);
            Assert.AreEqual(originalSalt1 + 1, wal.Header.Salt1);
            Assert.AreNotEqual(originalSalt2, wal.Header.Salt2);

            List<WalFrame> frames = wal.ReadAllFrames();
            Assert.IsEmpty(frames);
        }
    }

    [TestMethod]
    public void WriteAheadLog_UpdateCheckpointTxId_UpdatesHeader()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Create();

            Assert.AreEqual(0ul, wal.Header.CheckpointTxId);

            wal.UpdateCheckpointTxId(12345);

            Assert.AreEqual(12345ul, wal.Header.CheckpointTxId);
        }

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Open();

            Assert.AreEqual(12345ul, wal.Header.CheckpointTxId);
        }
    }

    [TestMethod]
    public void WriteAheadLog_GetFramesByTransaction_GroupsCorrectly()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        byte[] testData = new byte[4096];

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 4096))
        {
            wal.Create();

            wal.WriteFrame(1, 0, 0x01, testData, WalFrameFlags.None);
            wal.WriteFrame(1, 1, 0x01, testData, WalFrameFlags.Commit);
            wal.WriteFrame(2, 2, 0x01, testData, WalFrameFlags.None);
            wal.WriteFrame(2, 3, 0x01, testData, WalFrameFlags.None);
            wal.WriteFrame(2, 4, 0x01, testData, WalFrameFlags.Commit);
            wal.WriteFrame(3, 5, 0x01, testData, WalFrameFlags.None);

            Dictionary<ulong, List<WalFrame>> txFrames = wal.GetFramesByTransaction();

            Assert.HasCount(3, txFrames);
            Assert.HasCount(2, txFrames[1]);
            Assert.HasCount(3, txFrames[2]);
            Assert.HasCount(1, txFrames[3]);
        }
    }

    [TestMethod]
    public void WriteAheadLog_GetCommittedTransactions_ReturnsOnlyCommitted()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        byte[] testData = new byte[4096];

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 4096))
        {
            wal.Create();

            wal.WriteFrame(1, 0, 0x01, testData, WalFrameFlags.Commit);
            wal.WriteFrame(2, 1, 0x01, testData, WalFrameFlags.None);
            wal.WriteFrame(3, 2, 0x01, testData, WalFrameFlags.Commit);
            wal.WriteFrame(4, 3, 0x01, testData, WalFrameFlags.None);

            HashSet<ulong> committed = wal.GetCommittedTransactions();

            Assert.HasCount(2, committed);
            Assert.Contains(1ul, committed);
            Assert.Contains(3ul, committed);
            Assert.DoesNotContain(2ul, committed);
            Assert.DoesNotContain(4ul, committed);
        }
    }

    [TestMethod]
    public void WriteAheadLog_MultipleTransactions_InterleavedWrites()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        byte[] testData = new byte[4096];

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 4096))
        {
            wal.Create();

            wal.WriteFrame(1, 0, 0x01, testData, WalFrameFlags.None);
            wal.WriteFrame(2, 1, 0x01, testData, WalFrameFlags.None);
            wal.WriteFrame(1, 2, 0x01, testData, WalFrameFlags.Commit);
            wal.WriteFrame(2, 3, 0x01, testData, WalFrameFlags.Commit);

            List<WalFrame> frames = wal.ReadAllFrames();

            Assert.HasCount(4, frames);
            Assert.AreEqual(1ul, frames[0].TxId);
            Assert.AreEqual(2ul, frames[1].TxId);
            Assert.AreEqual(1ul, frames[2].TxId);
            Assert.AreEqual(2ul, frames[3].TxId);

            HashSet<ulong> committed = wal.GetCommittedTransactions();
            Assert.HasCount(2, committed);
        }
    }

    [TestMethod]
    public void WriteAheadLog_FrameDataIntegrity_LargeData()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        int pageSize = 8192;
        byte[] testData = new byte[pageSize];

        for (int i = 0; i < pageSize; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        using (WriteAheadLog wal = new WriteAheadLog(walPath, pageSize))
        {
            wal.Create();
            wal.WriteFrame(1, 0, 0x01, testData, WalFrameFlags.Commit);
            wal.Flush();
        }

        using (WriteAheadLog wal = new WriteAheadLog(walPath, pageSize))
        {
            wal.Open();

            List<WalFrame> frames = wal.ReadAllFrames();

            Assert.HasCount(1, frames);
            Assert.HasCount(pageSize, frames[0].Data);
            CollectionAssert.AreEqual(testData, frames[0].Data);
        }
    }

    [TestMethod]
    public void WriteAheadLog_SmallerThanPageSize_PaddedCorrectly()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        int pageSize = 8192;
        byte[] smallData = new byte[100];
        for (int i = 0; i < smallData.Length; i++)
        {
            smallData[i] = (byte)i;
        }

        using (WriteAheadLog wal = new WriteAheadLog(walPath, pageSize))
        {
            wal.Create();
            wal.WriteFrame(1, 0, 0x01, smallData, WalFrameFlags.Commit);
            wal.Flush();
        }

        using (WriteAheadLog wal = new WriteAheadLog(walPath, pageSize))
        {
            wal.Open();

            List<WalFrame> frames = wal.ReadAllFrames();

            Assert.HasCount(1, frames);
            Assert.AreEqual(100, frames[0].DataLength);
            Assert.HasCount(100, frames[0].Data);
            CollectionAssert.AreEqual(smallData, frames[0].Data);
        }
    }

    #endregion

    #region Salt Mechanism Tests

    [TestMethod]
    public void WriteAheadLog_Frames_ContainHeaderSalts()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        byte[] testData = new byte[4096];

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 4096))
        {
            wal.Create();
            uint expectedSalt1 = wal.Header.Salt1;
            uint expectedSalt2 = wal.Header.Salt2;

            wal.WriteFrame(1, 0, 0x01, testData, WalFrameFlags.Commit);

            List<WalFrame> frames = wal.ReadAllFrames();

            Assert.HasCount(1, frames);
            Assert.AreEqual(expectedSalt1, frames[0].Salt1);
            Assert.AreEqual(expectedSalt2, frames[0].Salt2);
        }
    }

    [TestMethod]
    public void WriteAheadLog_SaltMismatch_StopsFrameScan()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        int pageSize = 4096;
        byte[] testData = new byte[pageSize];

        using (WriteAheadLog wal = new WriteAheadLog(walPath, pageSize))
        {
            wal.Create();
            wal.WriteFrame(1, 0, 0x01, testData, WalFrameFlags.Commit);
            wal.WriteFrame(2, 1, 0x01, testData, WalFrameFlags.Commit);
            wal.Flush();
        }

        using (WriteAheadLog wal = new WriteAheadLog(walPath, pageSize))
        {
            wal.Open();
            wal.Truncate();
            wal.WriteFrame(3, 2, 0x01, testData, WalFrameFlags.Commit);

            List<WalFrame> frames = wal.ReadAllFrames();

            Assert.HasCount(1, frames);
            Assert.AreEqual(3ul, frames[0].TxId);
        }
    }

    [TestMethod]
    public void WriteAheadLog_AfterTruncate_OldFramesNotVisible()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        int pageSize = 4096;
        byte[] testData = new byte[pageSize];

        using (WriteAheadLog wal = new WriteAheadLog(walPath, pageSize))
        {
            wal.Create();
            wal.WriteFrame(1, 0, 0x01, testData, WalFrameFlags.Commit);
            wal.WriteFrame(2, 1, 0x01, testData, WalFrameFlags.Commit);

            Assert.AreEqual(2, wal.CurrentFrameNumber);

            wal.Truncate();

            List<WalFrame> frames = wal.ReadAllFrames();
            Assert.IsEmpty(frames);
        }
    }

    [TestMethod]
    public void WriteAheadLog_MultipleCheckpoints_SaltsIncrement()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        byte[] testData = new byte[4096];

        using (WriteAheadLog wal = new WriteAheadLog(walPath, 4096))
        {
            wal.Create();
            uint salt1After0 = wal.Header.Salt1;

            wal.WriteFrame(1, 0, 0x01, testData, WalFrameFlags.Commit);
            wal.Truncate();
            uint salt1After1 = wal.Header.Salt1;

            wal.WriteFrame(2, 1, 0x01, testData, WalFrameFlags.Commit);
            wal.Truncate();
            uint salt1After2 = wal.Header.Salt1;

            Assert.AreEqual(salt1After0 + 1, salt1After1);
            Assert.AreEqual(salt1After1 + 1, salt1After2);
        }
    }

    [TestMethod]
    public void WriteAheadLog_WriteTransactionBatch_AllFramesHaveCorrectSalts()
    {
        string walPath = Path.Combine(_testDirectory, "test.wal");
        int pageSize = 4096;

        using (WriteAheadLog wal = new WriteAheadLog(walPath, pageSize))
        {
            wal.Create();
            uint expectedSalt1 = wal.Header.Salt1;
            uint expectedSalt2 = wal.Header.Salt2;

            List<PendingPageWrite> writes = new List<PendingPageWrite>
            {
                new PendingPageWrite(0, new byte[pageSize], 0x01),
                new PendingPageWrite(1, new byte[pageSize], 0x01),
                new PendingPageWrite(2, new byte[pageSize], 0x01)
            };

            wal.WriteTransactionBatch(1, writes);

            List<WalFrame> frames = wal.ReadAllFrames();

            Assert.HasCount(3, frames);
            foreach (WalFrame frame in frames)
            {
                Assert.AreEqual(expectedSalt1, frame.Salt1);
                Assert.AreEqual(expectedSalt2, frame.Salt2);
            }

            Assert.AreEqual(WalFrameFlags.None, frames[0].Flags);
            Assert.AreEqual(WalFrameFlags.None, frames[1].Flags);
            Assert.AreEqual(WalFrameFlags.Commit, frames[2].Flags);
        }
    }

    #endregion
}
