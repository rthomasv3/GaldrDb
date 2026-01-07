using System;
using System.IO;
using GaldrDbCore.IO;

namespace GaldrDb.UnitTests;

[TestClass]
public class IOTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbTests_{Guid.NewGuid()}");
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

    [TestMethod]
    public void StandardPageIO_CreateAndWritePage_Success()
    {
        string filePath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        byte[] testData = new byte[pageSize];

        for (int i = 0; i < pageSize; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        using (StandardPageIO pageIO = new StandardPageIO(filePath, pageSize, true))
        {
            pageIO.WritePage(0, testData);
            pageIO.Flush();

            byte[] readData = new byte[pageSize];
            pageIO.ReadPage(0, readData);

            CollectionAssert.AreEqual(testData, readData);
        }

        bool result = File.Exists(filePath);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void StandardPageIO_WriteMultiplePages_Success()
    {
        string filePath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 4096;

        using (StandardPageIO pageIO = new StandardPageIO(filePath, pageSize, true))
        {
            for (int pageId = 0; pageId < 10; pageId++)
            {
                byte[] testData = new byte[pageSize];

                for (int i = 0; i < pageSize; i++)
                {
                    testData[i] = (byte)((pageId * 100 + i) % 256);
                }

                pageIO.WritePage(pageId, testData);
            }

            pageIO.Flush();

            for (int pageId = 0; pageId < 10; pageId++)
            {
                byte[] expectedData = new byte[pageSize];

                for (int i = 0; i < pageSize; i++)
                {
                    expectedData[i] = (byte)((pageId * 100 + i) % 256);
                }

                byte[] readData = new byte[pageSize];
                pageIO.ReadPage(pageId, readData);

                CollectionAssert.AreEqual(expectedData, readData);
            }
        }

        bool result = true;

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void MmapPageIO_CreateAndWritePage_Success()
    {
        if (!MmapPageIO.IsMmapSupported())
        {
            Assert.Inconclusive("Memory-mapped files not supported on this platform");
            return;
        }

        string filePath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        long initialSize = pageSize * 4;
        byte[] testData = new byte[pageSize];

        for (int i = 0; i < pageSize; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        using (MmapPageIO pageIO = new MmapPageIO(filePath, pageSize, initialSize, true))
        {
            pageIO.WritePage(0, testData);
            pageIO.Flush();

            byte[] readData = new byte[pageSize];
            pageIO.ReadPage(0, readData);

            CollectionAssert.AreEqual(testData, readData);
        }

        bool result = File.Exists(filePath);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void MmapPageIO_WriteMultiplePages_Success()
    {
        if (!MmapPageIO.IsMmapSupported())
        {
            Assert.Inconclusive("Memory-mapped files not supported on this platform");
            return;
        }

        string filePath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 4096;
        long initialSize = pageSize * 4;

        using (MmapPageIO pageIO = new MmapPageIO(filePath, pageSize, initialSize, true))
        {
            for (int pageId = 0; pageId < 10; pageId++)
            {
                byte[] testData = new byte[pageSize];

                for (int i = 0; i < pageSize; i++)
                {
                    testData[i] = (byte)((pageId * 100 + i) % 256);
                }

                pageIO.WritePage(pageId, testData);
            }

            pageIO.Flush();

            for (int pageId = 0; pageId < 10; pageId++)
            {
                byte[] expectedData = new byte[pageSize];

                for (int i = 0; i < pageSize; i++)
                {
                    expectedData[i] = (byte)((pageId * 100 + i) % 256);
                }

                byte[] readData = new byte[pageSize];
                pageIO.ReadPage(pageId, readData);

                CollectionAssert.AreEqual(expectedData, readData);
            }
        }

        bool result = true;

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void MmapPageIO_IsMmapSupported_ReturnsBoolean()
    {
        bool supported = MmapPageIO.IsMmapSupported();
        bool result = supported == true || supported == false;

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void StandardPageIO_Dispose_ClosesFile()
    {
        string filePath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 4096;
        byte[] testData = new byte[pageSize];

        StandardPageIO pageIO = new StandardPageIO(filePath, pageSize, true);
        pageIO.WritePage(0, testData);
        pageIO.Dispose();

        FileStream fs = null;
        bool canAccessFile = false;

        try
        {
            fs = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            canAccessFile = true;
        }
        catch
        {
            canAccessFile = false;
        }
        finally
        {
            if (fs != null)
            {
                fs.Dispose();
            }
        }

        Assert.IsTrue(canAccessFile);
    }

    [TestMethod]
    public void StandardPageIO_WritePage_WrongSize_ThrowsException()
    {
        string filePath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 4096;
        byte[] wrongSizeData = new byte[2048];

        ArgumentException exception = Assert.Throws<ArgumentException>(() =>
        {
            using (StandardPageIO pageIO = new StandardPageIO(filePath, pageSize, true))
            {
                pageIO.WritePage(0, wrongSizeData);
            }
        });

        bool result = exception != null;

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void MmapPageIO_WritePage_WrongSize_ThrowsException()
    {
        if (!MmapPageIO.IsMmapSupported())
        {
            Assert.Inconclusive("Memory-mapped files not supported on this platform");
            return;
        }

        string filePath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 4096;
        long initialSize = pageSize * 4;
        byte[] wrongSizeData = new byte[2048];

        ArgumentException exception = Assert.Throws<ArgumentException>(() =>
        {
            using (MmapPageIO pageIO = new MmapPageIO(filePath, pageSize, initialSize, true))
            {
                pageIO.WritePage(0, wrongSizeData);
            }
        });

        bool result = exception != null;

        Assert.IsTrue(result);
    }
}
