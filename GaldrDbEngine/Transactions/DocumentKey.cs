using System;

namespace GaldrDbEngine.Transactions;

/// <summary>
/// Identifies a document by its collection name and document ID.
/// Used as a key for tracking reads and writes in transactions.
/// </summary>
internal readonly struct DocumentKey : IEquatable<DocumentKey>
{
    public string CollectionName { get; }
    public int DocId { get; }

    public DocumentKey(string collectionName, int docId)
    {
        CollectionName = collectionName;
        DocId = docId;
    }

    public bool Equals(DocumentKey other)
    {
        return CollectionName == other.CollectionName && DocId == other.DocId;
    }

    public override bool Equals(object obj)
    {
        return obj is DocumentKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(CollectionName, DocId);
    }

    public static bool operator ==(DocumentKey left, DocumentKey right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(DocumentKey left, DocumentKey right)
    {
        return !left.Equals(right);
    }
}
