using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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
        ulong commitCSN = 5;
        ulong snapshotCSN = 10;
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(1, createdBy, commitCSN, location, null);

        bool result = version.IsVisibleTo(snapshotCSN);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void DocumentVersion_IsVisibleTo_CreatedAfterSnapshot_ReturnsFalse()
    {
        TxId createdBy = new TxId(15);
        ulong commitCSN = 15;
        ulong snapshotCSN = 10;
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(1, createdBy, commitCSN, location, null);

        bool result = version.IsVisibleTo(snapshotCSN);

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void DocumentVersion_IsVisibleTo_CreatedAtSnapshot_ReturnsTrue()
    {
        TxId createdBy = new TxId(10);
        ulong commitCSN = 10;
        ulong snapshotCSN = 10;
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(1, createdBy, commitCSN, location, null);

        bool result = version.IsVisibleTo(snapshotCSN);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void DocumentVersion_IsVisibleTo_DeletedAfterSnapshot_ReturnsTrue()
    {
        TxId createdBy = new TxId(5);
        ulong commitCSN = 5;
        ulong deletedCSN = 15;
        ulong snapshotCSN = 10;
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(1, createdBy, commitCSN, location, null);
        version.MarkDeleted(deletedCSN);

        bool result = version.IsVisibleTo(snapshotCSN);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void DocumentVersion_IsVisibleTo_DeletedBeforeSnapshot_ReturnsFalse()
    {
        TxId createdBy = new TxId(5);
        ulong commitCSN = 5;
        ulong deletedCSN = 8;
        ulong snapshotCSN = 10;
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(1, createdBy, commitCSN, location, null);
        version.MarkDeleted(deletedCSN);

        bool result = version.IsVisibleTo(snapshotCSN);

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void DocumentVersion_IsVisibleTo_DeletedAtSnapshot_ReturnsFalse()
    {
        TxId createdBy = new TxId(5);
        ulong commitCSN = 5;
        ulong deletedCSN = 10;
        ulong snapshotCSN = 10;
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(1, createdBy, commitCSN, location, null);
        version.MarkDeleted(deletedCSN);

        bool result = version.IsVisibleTo(snapshotCSN);

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void DocumentVersion_IsDeleted_NotDeleted_ReturnsFalse()
    {
        TxId createdBy = new TxId(5);
        ulong commitCSN = 5;
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(1, createdBy, commitCSN, location, null);

        Assert.IsFalse(version.IsDeleted);
    }

    [TestMethod]
    public void DocumentVersion_IsDeleted_Deleted_ReturnsTrue()
    {
        TxId createdBy = new TxId(5);
        ulong commitCSN = 5;
        DocumentLocation location = new DocumentLocation(1, 0);
        DocumentVersion version = new DocumentVersion(1, createdBy, commitCSN, location, null);
        version.MarkDeleted(10ul);

        Assert.IsTrue(version.IsDeleted);
    }

    [TestMethod]
    public void DocumentVersion_PreviousVersion_LinkedCorrectly()
    {
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        ulong csn1 = 5;
        ulong csn2 = 10;
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);

        DocumentVersion version1 = new DocumentVersion(1, tx1, csn1, location1, null);
        DocumentVersion version2 = new DocumentVersion(1, tx2, csn2, location2, version1);

        Assert.IsNull(version1.PreviousVersion);
        Assert.AreEqual(version1, version2.PreviousVersion);
    }

    [TestMethod]
    public void VersionIndex_AddVersion_FirstVersion_AddsSuccessfully()
    {
        VersionIndex index = new VersionIndex();
        TxId txId = new TxId(5);
        ulong commitCSN = 5;
        DocumentLocation location = new DocumentLocation(1, 0);

        index.AddVersion("TestCollection", 1, txId, commitCSN, location);

        Assert.IsTrue(index.HasVersion("TestCollection", 1));
    }

    [TestMethod]
    public void VersionIndex_GetLatestVersion_ReturnsLatest()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        ulong csn1 = 5;
        ulong csn2 = 10;
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);

        index.AddVersion("TestCollection", 1, tx1, csn1, location1);
        index.AddVersion("TestCollection", 1, tx2, csn2, location2);

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
        index.AddVersion("TestCollection", 1, new TxId(5), 5ul, new DocumentLocation(1, 0));

        DocumentVersion result = index.GetLatestVersion("TestCollection", 999);

        Assert.IsNull(result);
    }

    [TestMethod]
    public void VersionIndex_GetVisibleVersion_ReturnsVisibleVersion()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;
        DocumentLocation location1 = new DocumentLocation(1, 0);
        ulong snapshotCSN = 10;

        index.AddVersion("TestCollection", 1, tx1, csn1, location1);

        DocumentVersion result = index.GetVisibleVersion("TestCollection", 1, snapshotCSN);

        Assert.IsNotNull(result);
        Assert.AreEqual(tx1, result.CreatedBy);
    }

    [TestMethod]
    public void VersionIndex_GetVisibleVersion_LatestNotVisible_ReturnsPreviousVersion()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(15);
        ulong csn1 = 5;
        ulong csn2 = 15;
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);
        ulong snapshotCSN = 10;

        index.AddVersion("TestCollection", 1, tx1, csn1, location1);
        index.AddVersion("TestCollection", 1, tx2, csn2, location2);

        DocumentVersion result = index.GetVisibleVersion("TestCollection", 1, snapshotCSN);

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
        ulong csn1 = 15;
        ulong csn2 = 20;
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);
        ulong snapshotCSN = 10;

        index.AddVersion("TestCollection", 1, tx1, csn1, location1);
        index.AddVersion("TestCollection", 1, tx2, csn2, location2);

        DocumentVersion result = index.GetVisibleVersion("TestCollection", 1, snapshotCSN);

        Assert.IsNull(result);
    }

    [TestMethod]
    public void VersionIndex_GetVisibleVersion_DeletedVersion_ReturnsNull()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;
        ulong deletedCSN = 8;
        DocumentLocation location1 = new DocumentLocation(1, 0);
        ulong snapshotCSN = 10;

        index.AddVersion("TestCollection", 1, tx1, csn1, location1);
        index.MarkDeleted("TestCollection", 1, deletedCSN);

        DocumentVersion result = index.GetVisibleVersion("TestCollection", 1, snapshotCSN);

        Assert.IsNull(result);
    }

    [TestMethod]
    public void VersionIndex_GetVisibleVersion_DeletedAfterSnapshot_ReturnsVersion()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;
        ulong deletedCSN = 15;
        DocumentLocation location1 = new DocumentLocation(1, 0);
        ulong snapshotCSN = 10;

        index.AddVersion("TestCollection", 1, tx1, csn1, location1);
        index.MarkDeleted("TestCollection", 1, deletedCSN);

        DocumentVersion result = index.GetVisibleVersion("TestCollection", 1, snapshotCSN);

        Assert.IsNotNull(result);
        Assert.AreEqual(tx1, result.CreatedBy);
    }

    [TestMethod]
    public void VersionIndex_MarkDeleted_MarksLatestVersion()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;
        ulong deletedCSN = 10;
        DocumentLocation location1 = new DocumentLocation(1, 0);

        index.AddVersion("TestCollection", 1, tx1, csn1, location1);
        index.MarkDeleted("TestCollection", 1, deletedCSN);

        DocumentVersion latest = index.GetLatestVersion("TestCollection", 1);

        Assert.IsTrue(latest.IsDeleted);
        Assert.AreEqual(deletedCSN, latest.DeletedCSN);
    }

    [TestMethod]
    public void VersionIndex_MarkDeleted_NonExistentDocument_DoesNotThrow()
    {
        VersionIndex index = new VersionIndex();

        index.MarkDeleted("TestCollection", 999, 10ul);

        Assert.IsFalse(index.HasVersion("TestCollection", 999));
    }

    [TestMethod]
    public void VersionIndex_GetVersionCount_ReturnsCorrectCount()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        TxId tx3 = new TxId(15);
        ulong csn1 = 5;
        ulong csn2 = 10;
        ulong csn3 = 15;
        DocumentLocation location = new DocumentLocation(1, 0);

        index.AddVersion("TestCollection", 1, tx1, csn1, location);
        index.AddVersion("TestCollection", 1, tx2, csn2, location);
        index.AddVersion("TestCollection", 1, tx3, csn3, location);

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
        index.AddVersion("TestCollection", 1, new TxId(5), 5ul, new DocumentLocation(1, 0));

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
        index.AddVersion("NewCollection", 1, new TxId(5), 5ul, new DocumentLocation(1, 0));

        Assert.IsTrue(index.HasVersion("NewCollection", 1));
    }

    [TestMethod]
    public void VersionIndex_MultipleCollections_IsolatedCorrectly()
    {
        VersionIndex index = new VersionIndex();
        TxId txId = new TxId(5);
        ulong csn = 5;
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);

        index.AddVersion("Collection1", 1, txId, csn, location1);
        index.AddVersion("Collection2", 1, txId, csn, location2);

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
        ulong csn1 = 5;
        ulong csn2 = 10;
        ulong csn3 = 15;
        DocumentLocation location1 = new DocumentLocation(1, 0);
        DocumentLocation location2 = new DocumentLocation(2, 0);
        DocumentLocation location3 = new DocumentLocation(3, 0);

        index.AddVersion("TestCollection", 1, tx1, csn1, location1);
        index.AddVersion("TestCollection", 1, tx2, csn2, location2);
        index.AddVersion("TestCollection", 1, tx3, csn3, location3);

        DocumentVersion latest = index.GetLatestVersion("TestCollection", 1);

        Assert.AreEqual(tx3, latest.CreatedBy);
        Assert.AreEqual(tx2, latest.PreviousVersion.CreatedBy);
        Assert.AreEqual(tx1, latest.PreviousVersion.PreviousVersion.CreatedBy);
        Assert.IsNull(latest.PreviousVersion.PreviousVersion.PreviousVersion);
    }

    #region GetAllVisibleVersions Tests

    [TestMethod]
    public void VersionIndex_GetAllVisibleVersions_ReturnsVisibleVersions()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        ulong csn1 = 5;
        ulong csn2 = 10;
        ulong snapshotCSN = 15;

        index.AddVersion("TestCollection", 1, tx1, csn1, new DocumentLocation(1, 0));
        index.AddVersion("TestCollection", 2, tx2, csn2, new DocumentLocation(2, 0));

        List<DocumentVersion> versions = index.GetAllVisibleVersions("TestCollection", snapshotCSN);

        Assert.HasCount(2, versions);
    }

    [TestMethod]
    public void VersionIndex_GetAllVisibleVersions_ExcludesDeletedVersions()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;
        ulong deletedCSN = 10;
        ulong snapshotCSN = 15;

        index.AddVersion("TestCollection", 1, tx1, csn1, new DocumentLocation(1, 0));
        index.AddVersion("TestCollection", 2, tx1, csn1, new DocumentLocation(2, 0));
        index.MarkDeleted("TestCollection", 1, deletedCSN);

        List<DocumentVersion> versions = index.GetAllVisibleVersions("TestCollection", snapshotCSN);

        Assert.HasCount(1, versions);
        Assert.AreEqual(2, versions[0].DocumentId);
    }

    [TestMethod]
    public void VersionIndex_GetAllVisibleVersions_EmptyCollection_ReturnsEmptyList()
    {
        VersionIndex index = new VersionIndex();
        ulong snapshotCSN = 10;

        List<DocumentVersion> versions = index.GetAllVisibleVersions("NonExistent", snapshotCSN);

        Assert.IsEmpty(versions);
    }

    #endregion

    #region GetVisibleVersionsForDocIds Tests

    [TestMethod]
    public void VersionIndex_GetVisibleVersionsForDocIds_ReturnsMatchingVersions()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;
        ulong snapshotCSN = 10;

        index.AddVersion("TestCollection", 1, tx1, csn1, new DocumentLocation(1, 0));
        index.AddVersion("TestCollection", 2, tx1, csn1, new DocumentLocation(2, 0));
        index.AddVersion("TestCollection", 3, tx1, csn1, new DocumentLocation(3, 0));

        List<int> docIds = new List<int> { 1, 3 };
        List<DocumentVersion> versions = index.GetVisibleVersionsForDocIds("TestCollection", docIds, snapshotCSN);

        Assert.HasCount(2, versions);
    }

    [TestMethod]
    public void VersionIndex_GetVisibleVersionsForDocIds_NonExistentDocIds_ReturnsEmpty()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;
        ulong snapshotCSN = 10;

        index.AddVersion("TestCollection", 1, tx1, csn1, new DocumentLocation(1, 0));

        List<int> docIds = new List<int> { 99, 100 };
        List<DocumentVersion> versions = index.GetVisibleVersionsForDocIds("TestCollection", docIds, snapshotCSN);

        Assert.IsEmpty(versions);
    }

    #endregion

    #region GetDocumentIds Tests

    [TestMethod]
    public void VersionIndex_GetDocumentIds_ReturnsAllDocIds()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;

        index.AddVersion("TestCollection", 1, tx1, csn1, new DocumentLocation(1, 0));
        index.AddVersion("TestCollection", 5, tx1, csn1, new DocumentLocation(2, 0));
        index.AddVersion("TestCollection", 10, tx1, csn1, new DocumentLocation(3, 0));

        List<int> docIds = index.GetDocumentIds("TestCollection");

        Assert.HasCount(3, docIds);
        Assert.Contains(1, docIds);
        Assert.Contains(5, docIds);
        Assert.Contains(10, docIds);
    }

    [TestMethod]
    public void VersionIndex_GetDocumentIds_EmptyCollection_ReturnsEmptyList()
    {
        VersionIndex index = new VersionIndex();

        List<int> docIds = index.GetDocumentIds("NonExistent");

        Assert.IsEmpty(docIds);
    }

    #endregion

    #region Collection Metadata Tests

    [TestMethod]
    public void VersionIndex_GetCollectionNames_ReturnsAllCollections()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;

        index.AddVersion("Collection1", 1, tx1, csn1, new DocumentLocation(1, 0));
        index.AddVersion("Collection2", 1, tx1, csn1, new DocumentLocation(2, 0));
        index.AddVersion("Collection3", 1, tx1, csn1, new DocumentLocation(3, 0));

        List<string> names = index.GetCollectionNames();

        Assert.HasCount(3, names);
        Assert.Contains("Collection1", names);
        Assert.Contains("Collection2", names);
        Assert.Contains("Collection3", names);
    }

    [TestMethod]
    public void VersionIndex_GetDocumentCount_ReturnsCorrectCount()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;

        index.AddVersion("TestCollection", 1, tx1, csn1, new DocumentLocation(1, 0));
        index.AddVersion("TestCollection", 2, tx1, csn1, new DocumentLocation(2, 0));
        index.AddVersion("TestCollection", 3, tx1, csn1, new DocumentLocation(3, 0));

        int count = index.GetDocumentCount("TestCollection");

        Assert.AreEqual(3, count);
    }

    [TestMethod]
    public void VersionIndex_GetDocumentCount_EmptyCollection_ReturnsZero()
    {
        VersionIndex index = new VersionIndex();

        int count = index.GetDocumentCount("NonExistent");

        Assert.AreEqual(0, count);
    }

    [TestMethod]
    public void VersionIndex_MultiVersionDocumentCount_TracksCorrectly()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        ulong csn1 = 5;
        ulong csn2 = 10;

        Assert.AreEqual(0, index.MultiVersionDocumentCount);

        index.AddVersion("TestCollection", 1, tx1, csn1, new DocumentLocation(1, 0));
        Assert.AreEqual(0, index.MultiVersionDocumentCount);

        index.AddVersion("TestCollection", 1, tx2, csn2, new DocumentLocation(2, 0));
        Assert.AreEqual(1, index.MultiVersionDocumentCount);

        index.AddVersion("TestCollection", 2, tx1, csn1, new DocumentLocation(3, 0));
        Assert.AreEqual(1, index.MultiVersionDocumentCount);

        index.AddVersion("TestCollection", 2, tx2, csn2, new DocumentLocation(4, 0));
        Assert.AreEqual(2, index.MultiVersionDocumentCount);
    }

    #endregion

    #region ValidateVersions and AddVersions Tests

    [TestMethod]
    public void VersionIndex_ValidateVersions_NoConflict_Succeeds()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        TxId snapshotTxId = new TxId(8);
        ulong csn1 = 5;
        ulong csn2 = 10;

        index.AddVersion("TestCollection", 1, tx1, csn1, new DocumentLocation(1, 0));

        List<VersionOperation> operations = new List<VersionOperation>
        {
            new VersionOperation("TestCollection", 2, new DocumentLocation(2, 0), false, null)
        };

        index.ValidateVersions(tx2, snapshotTxId, operations);
        index.AddVersions(tx2, csn2, operations);

        Assert.IsTrue(index.HasVersion("TestCollection", 2));
    }

    [TestMethod]
    public void VersionIndex_ValidateVersions_Conflict_Throws()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        TxId tx3 = new TxId(15);
        TxId snapshotTxId = new TxId(8);
        ulong csn1 = 5;
        ulong csn2 = 10;

        index.AddVersion("TestCollection", 1, tx1, csn1, new DocumentLocation(1, 0));
        index.AddVersion("TestCollection", 1, tx2, csn2, new DocumentLocation(2, 0));

        List<VersionOperation> operations = new List<VersionOperation>
        {
            new VersionOperation("TestCollection", 1, new DocumentLocation(3, 0), false, tx1)
        };

        Assert.ThrowsExactly<WriteConflictException>(() =>
        {
            index.ValidateVersions(tx3, snapshotTxId, operations);
        });

        // Verify AddVersions was not called - doc 1 should still be at tx2's version
        DocumentVersion version = index.GetLatestVersion("TestCollection", 1);
        Assert.AreEqual(tx2, version.CreatedBy);
    }

    #endregion

    #region UnlinkVersion Tests

    [TestMethod]
    public void VersionIndex_UnlinkVersion_RemovesFromChain()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        TxId tx2 = new TxId(10);
        ulong csn1 = 5;
        ulong csn2 = 10;

        index.AddVersion("TestCollection", 1, tx1, csn1, new DocumentLocation(1, 0));
        index.AddVersion("TestCollection", 1, tx2, csn2, new DocumentLocation(2, 0));

        DocumentVersion latest = index.GetLatestVersion("TestCollection", 1);
        DocumentVersion previous = latest.PreviousVersion;

        Assert.AreEqual(2, index.GetVersionCount("TestCollection", 1));

        index.UnlinkVersion("TestCollection", 1, latest, previous);

        Assert.AreEqual(1, index.GetVersionCount("TestCollection", 1));
    }

    [TestMethod]
    public void VersionIndex_UnlinkVersion_LastVersion_RemovesDocument()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;

        index.AddVersion("TestCollection", 1, tx1, csn1, new DocumentLocation(1, 0));

        DocumentVersion version = index.GetLatestVersion("TestCollection", 1);

        index.UnlinkVersion("TestCollection", 1, null, version);

        Assert.IsFalse(index.HasVersion("TestCollection", 1));
    }

    #endregion

    #region Concurrent Access Tests

    [TestMethod]
    public void VersionIndex_ConcurrentReads_NoErrors()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;
        ulong snapshotCSN = 10;

        for (int i = 0; i < 100; i++)
        {
            index.AddVersion("TestCollection", i, tx1, csn1, new DocumentLocation(i, 0));
        }

        int errors = 0;
        List<Task> tasks = new List<Task>();

        for (int i = 0; i < 20; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 100; j++)
                {
                    DocumentVersion version = index.GetVisibleVersion("TestCollection", j % 100, snapshotCSN);
                    if (version == null)
                    {
                        Interlocked.Increment(ref errors);
                    }
                }
            }));
        }

        Task.WaitAll(tasks.ToArray());

        Assert.AreEqual(0, errors, "Concurrent reads should not produce errors");
    }

    [TestMethod]
    public void VersionIndex_ConcurrentReadsAndWrites_NoExceptions()
    {
        VersionIndex index = new VersionIndex();
        ulong snapshotCSN = 1000;
        int nextTxId = 1;
        object txIdLock = new object();

        for (int i = 0; i < 50; i++)
        {
            index.AddVersion("TestCollection", i, new TxId((ulong)i + 1), (ulong)i + 1, new DocumentLocation(i, 0));
        }
        nextTxId = 51;

        int exceptions = 0;
        List<Task> tasks = new List<Task>();

        for (int i = 0; i < 10; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 50; j++)
                {
                    try
                    {
                        index.GetVisibleVersion("TestCollection", j, snapshotCSN);
                        index.GetAllVisibleVersions("TestCollection", snapshotCSN);
                        index.GetDocumentIds("TestCollection");
                        index.HasVersion("TestCollection", j);
                        int unused = index.MultiVersionDocumentCount;
                    }
                    catch
                    {
                        Interlocked.Increment(ref exceptions);
                    }
                }
            }));
        }

        for (int i = 0; i < 5; i++)
        {
            int writerIndex = i;
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 20; j++)
                {
                    try
                    {
                        int txId;
                        lock (txIdLock)
                        {
                            txId = nextTxId++;
                        }
                        int docId = (writerIndex * 20 + j) % 50;
                        index.AddVersion("TestCollection", docId, new TxId((ulong)txId), (ulong)txId, new DocumentLocation(txId, 0));
                    }
                    catch
                    {
                        Interlocked.Increment(ref exceptions);
                    }
                }
            }));
        }

        Task.WaitAll(tasks.ToArray());

        Assert.AreEqual(0, exceptions, "Concurrent reads and writes should not throw exceptions");
    }

    [TestMethod]
    public void VersionIndex_ConcurrentReads_GetAllVisibleVersions_Consistent()
    {
        VersionIndex index = new VersionIndex();
        TxId tx1 = new TxId(5);
        ulong csn1 = 5;
        ulong snapshotCSN = 10;
        int documentCount = 100;

        for (int i = 0; i < documentCount; i++)
        {
            index.AddVersion("TestCollection", i, tx1, csn1, new DocumentLocation(i, 0));
        }

        int inconsistencies = 0;
        List<Task> tasks = new List<Task>();

        for (int i = 0; i < 20; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 50; j++)
                {
                    List<DocumentVersion> versions = index.GetAllVisibleVersions("TestCollection", snapshotCSN);
                    if (versions.Count != documentCount)
                    {
                        Interlocked.Increment(ref inconsistencies);
                    }
                }
            }));
        }

        Task.WaitAll(tasks.ToArray());

        Assert.AreEqual(0, inconsistencies, "All concurrent reads should see consistent document count");
    }

    [TestMethod]
    public void VersionIndex_HighContentionWriters_NoDeadlock()
    {
        VersionIndex index = new VersionIndex();
        int nextTxId = 1;
        object txIdLock = new object();

        index.EnsureCollection("TestCollection");

        int completedOperations = 0;
        List<Task> tasks = new List<Task>();
        CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        for (int i = 0; i < 10; i++)
        {
            int writerIndex = i;
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 100 && !cts.Token.IsCancellationRequested; j++)
                {
                    int txId;
                    lock (txIdLock)
                    {
                        txId = nextTxId++;
                    }
                    index.AddVersion("TestCollection", writerIndex, new TxId((ulong)txId), (ulong)txId, new DocumentLocation(txId, 0));
                    Interlocked.Increment(ref completedOperations);
                }
            }));
        }

        bool completedInTime = Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(10));

        Assert.IsTrue(completedInTime, "All writers should complete without deadlock");
        Assert.AreEqual(1000, completedOperations, "All operations should complete");
    }

    [TestMethod]
    public void VersionIndex_MixedOperations_StressTest()
    {
        VersionIndex index = new VersionIndex();
        int nextTxId = 1;
        object txIdLock = new object();
        ulong snapshotCSN = 100000;

        for (int i = 0; i < 50; i++)
        {
            index.AddVersion("TestCollection", i, new TxId((ulong)(i + 1)), (ulong)(i + 1), new DocumentLocation(i, 0));
        }
        nextTxId = 51;

        int errors = 0;
        List<Task> tasks = new List<Task>();

        for (int i = 0; i < 5; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 100; j++)
                {
                    try
                    {
                        index.GetVisibleVersion("TestCollection", j % 50, snapshotCSN);
                    }
                    catch
                    {
                        Interlocked.Increment(ref errors);
                    }
                }
            }));
        }

        for (int i = 0; i < 5; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 100; j++)
                {
                    try
                    {
                        index.GetAllVisibleVersions("TestCollection", snapshotCSN);
                    }
                    catch
                    {
                        Interlocked.Increment(ref errors);
                    }
                }
            }));
        }

        for (int i = 0; i < 3; i++)
        {
            int writerIndex = i;
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 50; j++)
                {
                    try
                    {
                        int txId;
                        lock (txIdLock)
                        {
                            txId = nextTxId++;
                        }
                        index.AddVersion("TestCollection", writerIndex * 10 + (j % 10), new TxId((ulong)txId), (ulong)txId, new DocumentLocation(txId, 0));
                    }
                    catch
                    {
                        Interlocked.Increment(ref errors);
                    }
                }
            }));
        }

        for (int i = 0; i < 2; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 50; j++)
                {
                    try
                    {
                        index.GetDocumentIds("TestCollection");
                        index.GetCollectionNames();
                        index.GetDocumentCount("TestCollection");
                        int unused = index.MultiVersionDocumentCount;
                    }
                    catch
                    {
                        Interlocked.Increment(ref errors);
                    }
                }
            }));
        }

        Task.WaitAll(tasks.ToArray());

        Assert.AreEqual(0, errors, "Mixed concurrent operations should not produce errors");
    }

    #endregion
}
