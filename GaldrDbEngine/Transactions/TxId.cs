using System;

namespace GaldrDbEngine.Transactions;

public readonly struct TxId : IComparable<TxId>, IEquatable<TxId>
{
    public static readonly TxId None = new TxId(0);
    public static readonly TxId MaxValue = new TxId(ulong.MaxValue);

    private readonly ulong _value;

    public TxId(ulong value)
    {
        _value = value;
    }

    public ulong Value
    {
        get { return _value; }
    }

    public bool IsValid
    {
        get { return _value > 0; }
    }

    public int CompareTo(TxId other)
    {
        return _value.CompareTo(other._value);
    }

    public bool Equals(TxId other)
    {
        return _value == other._value;
    }

    public override bool Equals(object obj)
    {
        bool result = obj is TxId other && Equals(other);

        return result;
    }

    public override int GetHashCode()
    {
        return _value.GetHashCode();
    }

    public override string ToString()
    {
        return $"TxId({_value})";
    }

    public static bool operator ==(TxId left, TxId right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(TxId left, TxId right)
    {
        return !left.Equals(right);
    }

    public static bool operator <(TxId left, TxId right)
    {
        return left._value < right._value;
    }

    public static bool operator >(TxId left, TxId right)
    {
        return left._value > right._value;
    }

    public static bool operator <=(TxId left, TxId right)
    {
        return left._value <= right._value;
    }

    public static bool operator >=(TxId left, TxId right)
    {
        return left._value >= right._value;
    }

    public static TxId operator ++(TxId txId)
    {
        return new TxId(txId._value + 1);
    }
}
