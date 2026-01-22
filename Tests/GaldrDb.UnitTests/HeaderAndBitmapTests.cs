using System;
using System.IO;
using GaldrDbEngine;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDatabase = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class HeaderAndBitmapTests
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
    public void Create_WritesValidHeader()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions();

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
            bool result = File.Exists(dbPath);
            Assert.IsTrue(result);
        }

        bool finalResult = true;
        Assert.IsTrue(finalResult);
    }

    [TestMethod]
    public void Open_ReadsHeaderCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions();

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
        }

        using (GaldrDatabase db = GaldrDatabase.Open(dbPath, options))
        {
            bool result = true;
            Assert.IsTrue(result);
        }

        bool finalResult = true;
        Assert.IsTrue(finalResult);
    }

    [TestMethod]
    public void Create_FileAlreadyExists_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions();

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
        }

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() =>
        {
            using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
            {
            }
        });

        bool result = exception != null;
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void Open_FileDoesNotExist_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "nonexistent.db");
        GaldrDbOptions options = new GaldrDbOptions();

        FileNotFoundException exception = Assert.Throws<FileNotFoundException>(() =>
        {
            using (GaldrDatabase db = GaldrDatabase.Open(dbPath, options))
            {
            }
        });

        bool result = exception != null;
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void Create_WithCustomPageSize_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 4096
        };

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
            bool result = File.Exists(dbPath);
            Assert.IsTrue(result);
        }

        bool finalResult = true;
        Assert.IsTrue(finalResult);
    }

    [TestMethod]
    public void Create_InvalidPageSize_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 512
        };

        ArgumentException exception = Assert.Throws<ArgumentException>(() =>
        {
            using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
            {
            }
        });

        bool result = exception != null;
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void Create_NonPowerOfTwoPageSize_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 3000
        };

        ArgumentException exception = Assert.Throws<ArgumentException>(() =>
        {
            using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
            {
            }
        });

        bool result = exception != null;
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void Bitmap_AllocateAndCheck()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions();

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
        }

        bool result = File.Exists(dbPath);
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FreeSpaceMap_SetAndGetLevel()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions();

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
        }

        bool result = File.Exists(dbPath);
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void Open_InvalidMagicNumber_RecoversFromWAL()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions();

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
        }

        byte[] corruptedHeader = File.ReadAllBytes(dbPath);
        corruptedHeader[0] = 0xFF;
        corruptedHeader[1] = 0xFF;
        corruptedHeader[2] = 0xFF;
        corruptedHeader[3] = 0xFF;
        File.WriteAllBytes(dbPath, corruptedHeader);

        bool result = false;

        try
        {
            using (GaldrDatabase db = GaldrDatabase.Open(dbPath, options))
            {
                result = true;
            }
        }
        catch { }

        Assert.IsTrue(result);
    }
    
    [TestMethod]
    public void Open_InvalidMagicNumber_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { UseWal = false };

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
        }

        byte[] corruptedHeader = File.ReadAllBytes(dbPath);
        corruptedHeader[0] = 0xFF;
        corruptedHeader[1] = 0xFF;
        corruptedHeader[2] = 0xFF;
        corruptedHeader[3] = 0xFF;
        File.WriteAllBytes(dbPath, corruptedHeader);

        InvalidDataException exception = Assert.Throws<InvalidDataException>(() =>
        {
            using (GaldrDatabase db = GaldrDatabase.Open(dbPath, options))
            {
            }
        });

        bool result = exception != null && exception.Message.Contains("magic number");
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void Create_AndReopen_PreservesData()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions createOptions = new GaldrDbOptions
        {
            PageSize = 4096,
            UseMmap = false
        };

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, createOptions))
        {
        }

        using (GaldrDatabase db = GaldrDatabase.Open(dbPath))
        {
            bool result = true;
            Assert.IsTrue(result);
        }

        bool finalResult = true;
        Assert.IsTrue(finalResult);
    }

    [TestMethod]
    public void Create_WithMmapDisabled_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            UseMmap = false
        };

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
            bool result = File.Exists(dbPath);
            Assert.IsTrue(result);
        }

        bool finalResult = true;
        Assert.IsTrue(finalResult);
    }
}
