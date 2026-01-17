using System;

namespace GaldrDbEngine.Transactions;

/// <summary>
/// Represents a unique transaction identifier used for MVCC versioning.
/// </summary>
public readonly struct TxId : IComparable<TxId>, IEquatable<TxId>
{
    /// <summary>
    /// Represents no transaction (value 0).
    /// </summary>
    public static readonly TxId None = new TxId(0);

    /// <summary>
    /// The maximum possible transaction ID.
    /// </summary>
    public static readonly TxId MaxValue = new TxId(ulong.MaxValue);

    private readonly ulong _value;

    /// <summary>
    /// Creates a new TxId with the specified value.
    /// </summary>
    /// <param name="value">The transaction ID value.</param>
    internal TxId(ulong value)
    {
        _value = value;
    }

    /// <summary>
    /// The underlying transaction ID value.
    /// </summary>
    public ulong Value
    {
        get { return _value; }
    }

    /// <summary>
    /// True if this represents a valid transaction (non-zero).
    /// </summary>
    public bool IsValid
    {
        get { return _value > 0; }
    }

    /// <inheritdoc/>
    public int CompareTo(TxId other)
    {
        return _value.CompareTo(other._value);
    }

    /// <inheritdoc/>
    public bool Equals(TxId other)
    {
        return _value == other._value;
    }

    /// <inheritdoc/>
    public override bool Equals(object obj)
    {
        bool result = obj is TxId other && Equals(other);

        return result;
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        return _value.GetHashCode();
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        return $"TxId({_value})";
    }

    /// <inheritdoc/>
    public static bool operator ==(TxId left, TxId right)
    {
        return left.Equals(right);
    }

    /// <inheritdoc/>
    public static bool operator !=(TxId left, TxId right)
    {
        return !left.Equals(right);
    }

    /// <inheritdoc/>
    public static bool operator <(TxId left, TxId right)
    {
        return left._value < right._value;
    }

    /// <inheritdoc/>
    public static bool operator >(TxId left, TxId right)
    {
        return left._value > right._value;
    }

    /// <inheritdoc/>
    public static bool operator <=(TxId left, TxId right)
    {
        return left._value <= right._value;
    }

    /// <inheritdoc/>
    public static bool operator >=(TxId left, TxId right)
    {
        return left._value >= right._value;
    }

    /// <inheritdoc/>
    public static TxId operator ++(TxId txId)
    {
        return new TxId(txId._value + 1);
    }
}
