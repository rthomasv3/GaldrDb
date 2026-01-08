using GaldrDbEngine.MVCC;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class VersionIndexTests
{
    [TestMethod]
    public void DocumentVersion_IsVisibleTo_CreatedBeforeSnapshot_ReturnsTrue()
    {
        TxId createdBy = new TxId(5);
        TxId snapshotTxId = new TxId(10);
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(createdBy, location, null);

        bool result = version.IsVisibleTo(snapshotTxId);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void DocumentVersion_IsVisibleTo_CreatedAfterSnapshot_ReturnsFalse()
    {
        TxId createdBy = new TxId(15);
        TxId snapshotTxId = new TxId(10);
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(createdBy, location, null);

        bool result = version.IsVisibleTo(snapshotTxId);

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void DocumentVersion_IsVisibleTo_CreatedAtSnapshot_ReturnsTrue()
    {
        TxId createdBy = new TxId(10);
        TxId snapshotTxId = new TxId(10);
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(createdBy, location, null);

        bool result = version.IsVisibleTo(snapshotTxId);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void DocumentVersion_IsVisibleTo_DeletedAfterSnapshot_ReturnsTrue()
    {
        TxId createdBy = new TxId(5);
        TxId deletedBy = new TxId(15);
        TxId snapshotTxId = new TxId(10);
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(createdBy, location, null);
        version.MarkDeleted(deletedBy);

        bool result = version.IsVisibleTo(snapshotTxId);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void DocumentVersion_IsVisibleTo_DeletedBeforeSnapshot_ReturnsFalse()
    {
        TxId createdBy = new TxId(5);
        TxId deletedBy = new TxId(8);
        TxId snapshotTxId = new TxId(10);
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(createdBy, location, null);
        version.MarkDeleted(deletedBy);

        bool result = version.IsVisibleTo(snapshotTxId);

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void DocumentVersion_IsVisibleTo_DeletedAtSnapshot_ReturnsFalse()
    {
        TxId createdBy = new TxId(5);
        TxId deletedBy = new TxId(10);
        TxId snapshotTxId = new TxId(10);
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(createdBy, location, null);
        version.MarkDeleted(deletedBy);

        bool result = version.IsVisibleTo(snapshotTxId);

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void DocumentVersion_IsDeleted_NotDeleted_ReturnsFalse()
    {
        TxId createdBy = new TxId(5);
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(createdBy, location, null);

        Assert.IsFalse(version.IsDeleted);
    }

    [TestMethod]
    public void DocumentVersion_IsDeleted_Deleted_ReturnsTrue()
    {
        TxId createdBy = new TxId(5);
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(createdBy, location, null);
        version.MarkDeleted(new TxId(10));

        Assert.IsTrue(version.IsDeleted);
    }

    [TestMethod]
    public void DocumentVersion_PreviousVersion_LinkedCorrectly()
    {
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);

        DocumentVersion version1 = new DocumentVersion(tx1, location1, null);
        DocumentVersion version2 = new DocumentVersion(tx2, location2, version1);

        Assert.IsNull(version1.PreviousVersion);
        Assert.AreEqual(version1, version2.PreviousVersion);
    }

    [TestMethod]
    public void VersionIndex_AddVersion_FirstVersion_AddsSuccessfully()
    {
        VersionIndex index = new VersionIndex();
        TxId txId = new TxId(5);
        DocumentLocation location = new DocumentLocation(1, 0);

        index.AddVersion("TestCollection", 1, txId, location);

        Assert.IsTrue(index.HasVersion("TestCollection", 1));
    }

    [TestMethod]
    public void VersionIndex_GetLatestVersion_ReturnsLatest()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);

        index.AddVersion("TestCollection", 1, tx1, location1);
        index.AddVersion("TestCollection", 1, tx2, location2);

        DocumentVersion latest = index.GetLatestVersion("TestCollection", 1);

        Assert.IsNotNull(latest);
        Assert.AreEqual(tx2, latest.CreatedBy);
        Assert.AreEqual(location2, latest.Location);
    }

    [TestMethod]
    public void VersionIndex_GetLatestVersion_NonExistentCollection_ReturnsNull()
    {
        VersionIndex index = new VersionIndex();

        DocumentVersion result = index.GetLatestVersion("NonExistent", 1);

        Assert.IsNull(result);
    }

    [TestMethod]
    public void VersionIndex_GetLatestVersion_NonExistentDocument_ReturnsNull()
    {
        VersionIndex index = new VersionIndex();
        index.AddVersion("TestCollection", 1, new TxId(5), new DocumentLocation(1, 0));

        DocumentVersion result = index.GetLatestVersion("TestCollection", 999);

        Assert.IsNull(result);
    }

    [TestMethod]
    public void VersionIndex_GetVisibleVersion_ReturnsVisibleVersion()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        DocumentLocation location1 = new DocumentLocation(1, 0);
        TxId snapshotTxId = new TxId(10);

        index.AddVersion("TestCollection", 1, tx1, location1);

        DocumentVersion result = index.GetVisibleVersion("TestCollection", 1, snapshotTxId);

        Assert.IsNotNull(result);
        Assert.AreEqual(tx1, result.CreatedBy);
    }

    [TestMethod]
    public void VersionIndex_GetVisibleVersion_LatestNotVisible_ReturnsPreviousVersion()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(15);
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);
        TxId snapshotTxId = new TxId(10);

        index.AddVersion("TestCollection", 1, tx1, location1);
        index.AddVersion("TestCollection", 1, tx2, location2);

        DocumentVersion result = index.GetVisibleVersion("TestCollection", 1, snapshotTxId);

        Assert.IsNotNull(result);
        Assert.AreEqual(tx1, result.CreatedBy);
        Assert.AreEqual(location1, result.Location);
    }

    [TestMethod]
    public void VersionIndex_GetVisibleVersion_AllVersionsNotVisible_ReturnsNull()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(15);
        TxId tx2 = new TxId(20);
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);
        TxId snapshotTxId = new TxId(10);

        index.AddVersion("TestCollection", 1, tx1, location1);
        index.AddVersion("TestCollection", 1, tx2, location2);

        DocumentVersion result = index.GetVisibleVersion("TestCollection", 1, snapshotTxId);

        Assert.IsNull(result);
    }

    [TestMethod]
    public void VersionIndex_GetVisibleVersion_DeletedVersion_ReturnsNull()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId deletedBy = new TxId(8);
        DocumentLocation location1 = new DocumentLocation(1, 0);
        TxId snapshotTxId = new TxId(10);

        index.AddVersion("TestCollection", 1, tx1, location1);
        index.MarkDeleted("TestCollection", 1, deletedBy);

        DocumentVersion result = index.GetVisibleVersion("TestCollection", 1, snapshotTxId);

        Assert.IsNull(result);
    }

    [TestMethod]
    public void VersionIndex_GetVisibleVersion_DeletedAfterSnapshot_ReturnsVersion()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId deletedBy = new TxId(15);
        DocumentLocation location1 = new DocumentLocation(1, 0);
        TxId snapshotTxId = new TxId(10);

        index.AddVersion("TestCollection", 1, tx1, location1);
        index.MarkDeleted("TestCollection", 1, deletedBy);

        DocumentVersion result = index.GetVisibleVersion("TestCollection", 1, snapshotTxId);

        Assert.IsNotNull(result);
        Assert.AreEqual(tx1, result.CreatedBy);
    }

    [TestMethod]
    public void VersionIndex_MarkDeleted_MarksLatestVersion()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId deletedBy = new TxId(10);
        DocumentLocation location1 = new DocumentLocation(1, 0);

        index.AddVersion("TestCollection", 1, tx1, location1);
        index.MarkDeleted("TestCollection", 1, deletedBy);

        DocumentVersion latest = index.GetLatestVersion("TestCollection", 1);

        Assert.IsTrue(latest.IsDeleted);
        Assert.AreEqual(deletedBy, latest.DeletedBy);
    }

    [TestMethod]
    public void VersionIndex_MarkDeleted_NonExistentDocument_DoesNotThrow()
    {
        VersionIndex index = new VersionIndex();

        index.MarkDeleted("TestCollection", 999, new TxId(10));

        Assert.IsFalse(index.HasVersion("TestCollection", 999));
    }

    [TestMethod]
    public void VersionIndex_GetVersionCount_ReturnsCorrectCount()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        TxId tx3 = new TxId(15);
        DocumentLocation location = new DocumentLocation(1, 0);

        index.AddVersion("TestCollection", 1, tx1, location);
        index.AddVersion("TestCollection", 1, tx2, location);
        index.AddVersion("TestCollection", 1, tx3, location);

        int count = index.GetVersionCount("TestCollection", 1);

        Assert.AreEqual(3, count);
    }

    [TestMethod]
    public void VersionIndex_GetVersionCount_NonExistentDocument_ReturnsZero()
    {
        VersionIndex index = new VersionIndex();

        int count = index.GetVersionCount("TestCollection", 999);

        Assert.AreEqual(0, count);
    }

    [TestMethod]
    public void VersionIndex_HasVersion_ExistingDocument_ReturnsTrue()
    {
        VersionIndex index = new VersionIndex();
        index.AddVersion("TestCollection", 1, new TxId(5), new DocumentLocation(1, 0));

        bool result = index.HasVersion("TestCollection", 1);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void VersionIndex_HasVersion_NonExistentDocument_ReturnsFalse()
    {
        VersionIndex index = new VersionIndex();

        bool result = index.HasVersion("TestCollection", 999);

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void VersionIndex_EnsureCollection_CreatesCollection()
    {
        VersionIndex index = new VersionIndex();

        index.EnsureCollection("NewCollection");
        index.AddVersion("NewCollection", 1, new TxId(5), new DocumentLocation(1, 0));

        Assert.IsTrue(index.HasVersion("NewCollection", 1));
    }

    [TestMethod]
    public void VersionIndex_MultipleCollections_IsolatedCorrectly()
    {
        VersionIndex index = new VersionIndex();
        TxId txId = new TxId(5);
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);

        index.AddVersion("Collection1", 1, txId, location1);
        index.AddVersion("Collection2", 1, txId, location2);

        DocumentVersion version1 = index.GetLatestVersion("Collection1", 1);
        DocumentVersion version2 = index.GetLatestVersion("Collection2", 1);

        Assert.AreEqual(location1, version1.Location);
        Assert.AreEqual(location2, version2.Location);
    }

    [TestMethod]
    public void VersionIndex_VersionChain_PreservesPreviousVersions()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        TxId tx3 = new TxId(15);
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);
        DocumentLocation location3 = new DocumentLocation(3, 0);

        index.AddVersion("TestCollection", 1, tx1, location1);
        index.AddVersion("TestCollection", 1, tx2, location2);
        index.AddVersion("TestCollection", 1, tx3, location3);

        DocumentVersion latest = index.GetLatestVersion("TestCollection", 1);

        Assert.AreEqual(tx3, latest.CreatedBy);
        Assert.AreEqual(tx2, latest.PreviousVersion.CreatedBy);
        Assert.AreEqual(tx1, latest.PreviousVersion.PreviousVersion.CreatedBy);
        Assert.IsNull(latest.PreviousVersion.PreviousVersion.PreviousVersion);
    }
}
