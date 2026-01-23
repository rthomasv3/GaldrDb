using System;
using GaldrDbEngine.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class IndexKeyEncoderTests
{
    [TestMethod]
    public void EncodeInt32_PositiveNumbers_MaintainsSortOrder()
    {
        byte[] encoded1 = IndexKeyEncoder.Encode(1, GaldrFieldType.Int32);
        byte[] encoded100 = IndexKeyEncoder.Encode(100, GaldrFieldType.Int32);
        byte[] encoded1000 = IndexKeyEncoder.Encode(1000, GaldrFieldType.Int32);

        Assert.IsLessThan(0, CompareBytes(encoded1, encoded100));
        Assert.IsLessThan(0, CompareBytes(encoded100, encoded1000));
        Assert.IsLessThan(0, CompareBytes(encoded1, encoded1000));
    }

    [TestMethod]
    public void EncodeInt32_NegativeNumbers_MaintainsSortOrder()
    {
        byte[] encodedNeg1000 = IndexKeyEncoder.Encode(-1000, GaldrFieldType.Int32);
        byte[] encodedNeg100 = IndexKeyEncoder.Encode(-100, GaldrFieldType.Int32);
        byte[] encodedNeg1 = IndexKeyEncoder.Encode(-1, GaldrFieldType.Int32);

        Assert.IsLessThan(0, CompareBytes(encodedNeg1000, encodedNeg100));
        Assert.IsLessThan(0, CompareBytes(encodedNeg100, encodedNeg1));
        Assert.IsLessThan(0, CompareBytes(encodedNeg1000, encodedNeg1));
    }

    [TestMethod]
    public void EncodeInt32_NegativeBeforePositive()
    {
        byte[] encodedNeg1 = IndexKeyEncoder.Encode(-1, GaldrFieldType.Int32);
        byte[] encoded0 = IndexKeyEncoder.Encode(0, GaldrFieldType.Int32);
        byte[] encoded1 = IndexKeyEncoder.Encode(1, GaldrFieldType.Int32);

        Assert.IsLessThan(0, CompareBytes(encodedNeg1, encoded0));
        Assert.IsLessThan(0, CompareBytes(encoded0, encoded1));
    }

    [TestMethod]
    public void EncodeInt32_MinAndMaxValues()
    {
        byte[] encodedMin = IndexKeyEncoder.Encode(int.MinValue, GaldrFieldType.Int32);
        byte[] encodedMax = IndexKeyEncoder.Encode(int.MaxValue, GaldrFieldType.Int32);
        byte[] encoded0 = IndexKeyEncoder.Encode(0, GaldrFieldType.Int32);

        Assert.IsLessThan(0, CompareBytes(encodedMin, encoded0));
        Assert.IsLessThan(0, CompareBytes(encoded0, encodedMax));
        Assert.IsLessThan(0, CompareBytes(encodedMin, encodedMax));
    }

    [TestMethod]
    public void EncodeInt32_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(42, GaldrFieldType.Int32);

        Assert.HasCount(5, encoded);
    }

    [TestMethod]
    public void EncodeInt64_PositiveNumbers_MaintainsSortOrder()
    {
        byte[] encoded1 = IndexKeyEncoder.Encode(1L, GaldrFieldType.Int64);
        byte[] encoded100 = IndexKeyEncoder.Encode(100L, GaldrFieldType.Int64);
        byte[] encodedLarge = IndexKeyEncoder.Encode(10000000000L, GaldrFieldType.Int64);

        Assert.IsLessThan(0, CompareBytes(encoded1, encoded100));
        Assert.IsLessThan(0, CompareBytes(encoded100, encodedLarge));
    }

    [TestMethod]
    public void EncodeInt64_NegativeBeforePositive()
    {
        byte[] encodedNeg = IndexKeyEncoder.Encode(-1L, GaldrFieldType.Int64);
        byte[] encoded0 = IndexKeyEncoder.Encode(0L, GaldrFieldType.Int64);
        byte[] encodedPos = IndexKeyEncoder.Encode(1L, GaldrFieldType.Int64);

        Assert.IsLessThan(0, CompareBytes(encodedNeg, encoded0));
        Assert.IsLessThan(0, CompareBytes(encoded0, encodedPos));
    }

    [TestMethod]
    public void EncodeInt64_MinAndMaxValues()
    {
        byte[] encodedMin = IndexKeyEncoder.Encode(long.MinValue, GaldrFieldType.Int64);
        byte[] encodedMax = IndexKeyEncoder.Encode(long.MaxValue, GaldrFieldType.Int64);

        Assert.IsLessThan(0, CompareBytes(encodedMin, encodedMax));
    }

    [TestMethod]
    public void EncodeInt64_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(42L, GaldrFieldType.Int64);

        Assert.HasCount(9, encoded);
    }

    [TestMethod]
    public void EncodeDouble_PositiveNumbers_MaintainsSortOrder()
    {
        byte[] encoded1 = IndexKeyEncoder.Encode(1.0, GaldrFieldType.Double);
        byte[] encoded2 = IndexKeyEncoder.Encode(2.5, GaldrFieldType.Double);
        byte[] encoded100 = IndexKeyEncoder.Encode(100.75, GaldrFieldType.Double);

        Assert.IsLessThan(0, CompareBytes(encoded1, encoded2));
        Assert.IsLessThan(0, CompareBytes(encoded2, encoded100));
    }

    [TestMethod]
    public void EncodeDouble_NegativeNumbers_MaintainsSortOrder()
    {
        byte[] encodedNeg100 = IndexKeyEncoder.Encode(-100.5, GaldrFieldType.Double);
        byte[] encodedNeg1 = IndexKeyEncoder.Encode(-1.0, GaldrFieldType.Double);

        Assert.IsLessThan(0, CompareBytes(encodedNeg100, encodedNeg1));
    }

    [TestMethod]
    public void EncodeDouble_NegativeBeforePositive()
    {
        byte[] encodedNeg = IndexKeyEncoder.Encode(-1.0, GaldrFieldType.Double);
        byte[] encoded0 = IndexKeyEncoder.Encode(0.0, GaldrFieldType.Double);
        byte[] encodedPos = IndexKeyEncoder.Encode(1.0, GaldrFieldType.Double);

        Assert.IsLessThan(0, CompareBytes(encodedNeg, encoded0));
        Assert.IsLessThan(0, CompareBytes(encoded0, encodedPos));
    }

    [TestMethod]
    public void EncodeDouble_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(3.14159, GaldrFieldType.Double);

        Assert.HasCount(9, encoded);
    }

    [TestMethod]
    public void EncodeString_MaintainsLexicographicOrder()
    {
        byte[] encodedA = IndexKeyEncoder.Encode("apple", GaldrFieldType.String);
        byte[] encodedB = IndexKeyEncoder.Encode("banana", GaldrFieldType.String);
        byte[] encodedC = IndexKeyEncoder.Encode("cherry", GaldrFieldType.String);

        Assert.IsLessThan(0, CompareBytes(encodedA, encodedB));
        Assert.IsLessThan(0, CompareBytes(encodedB, encodedC));
    }

    [TestMethod]
    public void EncodeString_PrefixComesBeforeLongerString()
    {
        byte[] encodedApp = IndexKeyEncoder.Encode("app", GaldrFieldType.String);
        byte[] encodedApple = IndexKeyEncoder.Encode("apple", GaldrFieldType.String);

        Assert.IsLessThan(0, CompareBytes(encodedApp, encodedApple));
    }

    [TestMethod]
    public void EncodeString_EmptyString()
    {
        byte[] encoded = IndexKeyEncoder.Encode("", GaldrFieldType.String);

        Assert.HasCount(1, encoded);
        Assert.AreEqual(0x01, encoded[0]);
    }

    [TestMethod]
    public void EncodeString_CaseSensitive()
    {
        byte[] encodedLower = IndexKeyEncoder.Encode("apple", GaldrFieldType.String);
        byte[] encodedUpper = IndexKeyEncoder.Encode("Apple", GaldrFieldType.String);

        Assert.AreNotEqual(0, CompareBytes(encodedLower, encodedUpper));
    }

    [TestMethod]
    public void EncodeBoolean_FalseBeforeTrue()
    {
        byte[] encodedFalse = IndexKeyEncoder.Encode(false, GaldrFieldType.Boolean);
        byte[] encodedTrue = IndexKeyEncoder.Encode(true, GaldrFieldType.Boolean);

        Assert.IsLessThan(0, CompareBytes(encodedFalse, encodedTrue));
    }

    [TestMethod]
    public void EncodeBoolean_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(true, GaldrFieldType.Boolean);

        Assert.HasCount(2, encoded);
    }

    [TestMethod]
    public void EncodeDateTime_MaintainsSortOrder()
    {
        DateTime date1 = new DateTime(2020, 1, 1);
        DateTime date2 = new DateTime(2021, 6, 15);
        DateTime date3 = new DateTime(2025, 12, 31);

        byte[] encoded1 = IndexKeyEncoder.Encode(date1, GaldrFieldType.DateTime);
        byte[] encoded2 = IndexKeyEncoder.Encode(date2, GaldrFieldType.DateTime);
        byte[] encoded3 = IndexKeyEncoder.Encode(date3, GaldrFieldType.DateTime);

        Assert.IsLessThan(0, CompareBytes(encoded1, encoded2));
        Assert.IsLessThan(0, CompareBytes(encoded2, encoded3));
    }

    [TestMethod]
    public void EncodeDateTime_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(DateTime.Now, GaldrFieldType.DateTime);

        Assert.HasCount(9, encoded);
    }

    [TestMethod]
    public void EncodeDateTimeOffset_MaintainsSortOrder()
    {
        DateTimeOffset date1 = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero);
        DateTimeOffset date2 = new DateTimeOffset(2021, 6, 15, 12, 30, 0, TimeSpan.Zero);
        DateTimeOffset date3 = new DateTimeOffset(2025, 12, 31, 23, 59, 59, TimeSpan.Zero);

        byte[] encoded1 = IndexKeyEncoder.Encode(date1, GaldrFieldType.DateTimeOffset);
        byte[] encoded2 = IndexKeyEncoder.Encode(date2, GaldrFieldType.DateTimeOffset);
        byte[] encoded3 = IndexKeyEncoder.Encode(date3, GaldrFieldType.DateTimeOffset);

        Assert.IsLessThan(0, CompareBytes(encoded1, encoded2));
        Assert.IsLessThan(0, CompareBytes(encoded2, encoded3));
    }

    [TestMethod]
    public void EncodeDateTimeOffset_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(DateTimeOffset.Now, GaldrFieldType.DateTimeOffset);

        Assert.HasCount(17, encoded);
    }

    [TestMethod]
    public void EncodeGuid_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(Guid.NewGuid(), GaldrFieldType.Guid);

        Assert.HasCount(17, encoded);
    }

    [TestMethod]
    public void EncodeGuid_SameGuidProducesSameBytes()
    {
        Guid guid = Guid.NewGuid();
        byte[] encoded1 = IndexKeyEncoder.Encode(guid, GaldrFieldType.Guid);
        byte[] encoded2 = IndexKeyEncoder.Encode(guid, GaldrFieldType.Guid);

        Assert.AreEqual(0, CompareBytes(encoded1, encoded2));
    }

    [TestMethod]
    public void EncodeDecimal_PositiveNumbers_MaintainsSortOrder()
    {
        byte[] encoded1 = IndexKeyEncoder.Encode(1.0m, GaldrFieldType.Decimal);
        byte[] encoded2 = IndexKeyEncoder.Encode(2.5m, GaldrFieldType.Decimal);
        byte[] encoded100 = IndexKeyEncoder.Encode(100.75m, GaldrFieldType.Decimal);

        Assert.IsLessThan(0, CompareBytes(encoded1, encoded2));
        Assert.IsLessThan(0, CompareBytes(encoded2, encoded100));
    }

    [TestMethod]
    public void EncodeDecimal_NegativeBeforePositive()
    {
        byte[] encodedNeg = IndexKeyEncoder.Encode(-1.0m, GaldrFieldType.Decimal);
        byte[] encoded0 = IndexKeyEncoder.Encode(0.0m, GaldrFieldType.Decimal);
        byte[] encodedPos = IndexKeyEncoder.Encode(1.0m, GaldrFieldType.Decimal);

        Assert.IsLessThan(0, CompareBytes(encodedNeg, encoded0));
        Assert.IsLessThan(0, CompareBytes(encoded0, encodedPos));
    }

    [TestMethod]
    public void EncodeDecimal_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(123.456m, GaldrFieldType.Decimal);

        Assert.HasCount(17, encoded);
    }

    [TestMethod]
    public void Encode_ComplexType_ThrowsNotSupportedException()
    {
        NotSupportedException exception = Assert.Throws<NotSupportedException>(() =>
        {
            IndexKeyEncoder.Encode(new object(), GaldrFieldType.Complex);
        });

        Assert.IsNotNull(exception);
    }

    [TestMethod]
    public void EncodePrefixEnd_ReturnsIncrementedLastByte()
    {
        byte[] prefixEnd = IndexKeyEncoder.EncodePrefixEnd("test");
        byte[] prefix = IndexKeyEncoder.Encode("test", GaldrFieldType.String);

        Assert.HasCount(prefix.Length, prefixEnd);
        Assert.AreEqual(prefix[prefix.Length - 1] + 1, prefixEnd[prefixEnd.Length - 1]);
    }

    [TestMethod]
    public void EncodePrefixEnd_StringsWithPrefixAreLessThanPrefixEnd()
    {
        byte[] prefixEnd = IndexKeyEncoder.EncodePrefixEnd("app");
        byte[] encodedApple = IndexKeyEncoder.Encode("apple", GaldrFieldType.String);
        byte[] encodedApplication = IndexKeyEncoder.Encode("application", GaldrFieldType.String);
        byte[] encodedBanana = IndexKeyEncoder.Encode("banana", GaldrFieldType.String);

        Assert.IsLessThan(0, CompareBytes(encodedApple, prefixEnd));
        Assert.IsLessThan(0, CompareBytes(encodedApplication, prefixEnd));
        Assert.IsLessThan(0, CompareBytes(prefixEnd, encodedBanana));
    }

    [TestMethod]
    public void EncodePrefixEnd_HighAsciiCharacter_Succeeds()
    {
        string highAscii = "test\u00FE";
        byte[] result = IndexKeyEncoder.EncodePrefixEnd(highAscii);

        Assert.IsNotNull(result);
    }

    [TestMethod]
    public void EncodePrefixEnd_EmptyString_ReturnsNull()
    {
        byte[] result = IndexKeyEncoder.EncodePrefixEnd("");

        Assert.IsNull(result);
    }

    [TestMethod]
    public void EncodeByte_MaintainsSortOrder()
    {
        byte[] encoded0 = IndexKeyEncoder.Encode((byte)0, GaldrFieldType.Byte);
        byte[] encoded100 = IndexKeyEncoder.Encode((byte)100, GaldrFieldType.Byte);
        byte[] encoded255 = IndexKeyEncoder.Encode((byte)255, GaldrFieldType.Byte);

        Assert.IsLessThan(0, CompareBytes(encoded0, encoded100));
        Assert.IsLessThan(0, CompareBytes(encoded100, encoded255));
    }

    [TestMethod]
    public void EncodeByte_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode((byte)42, GaldrFieldType.Byte);

        Assert.HasCount(2, encoded);
    }

    [TestMethod]
    public void EncodeSByte_MaintainsSortOrder()
    {
        byte[] encodedMin = IndexKeyEncoder.Encode(sbyte.MinValue, GaldrFieldType.SByte);
        byte[] encodedNeg1 = IndexKeyEncoder.Encode((sbyte)-1, GaldrFieldType.SByte);
        byte[] encoded0 = IndexKeyEncoder.Encode((sbyte)0, GaldrFieldType.SByte);
        byte[] encoded1 = IndexKeyEncoder.Encode((sbyte)1, GaldrFieldType.SByte);
        byte[] encodedMax = IndexKeyEncoder.Encode(sbyte.MaxValue, GaldrFieldType.SByte);

        Assert.IsLessThan(0, CompareBytes(encodedMin, encodedNeg1));
        Assert.IsLessThan(0, CompareBytes(encodedNeg1, encoded0));
        Assert.IsLessThan(0, CompareBytes(encoded0, encoded1));
        Assert.IsLessThan(0, CompareBytes(encoded1, encodedMax));
    }

    [TestMethod]
    public void EncodeSByte_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode((sbyte)42, GaldrFieldType.SByte);

        Assert.HasCount(2, encoded);
    }

    [TestMethod]
    public void EncodeInt16_MaintainsSortOrder()
    {
        byte[] encodedMin = IndexKeyEncoder.Encode(short.MinValue, GaldrFieldType.Int16);
        byte[] encodedNeg1 = IndexKeyEncoder.Encode((short)-1, GaldrFieldType.Int16);
        byte[] encoded0 = IndexKeyEncoder.Encode((short)0, GaldrFieldType.Int16);
        byte[] encoded1 = IndexKeyEncoder.Encode((short)1, GaldrFieldType.Int16);
        byte[] encodedMax = IndexKeyEncoder.Encode(short.MaxValue, GaldrFieldType.Int16);

        Assert.IsLessThan(0, CompareBytes(encodedMin, encodedNeg1));
        Assert.IsLessThan(0, CompareBytes(encodedNeg1, encoded0));
        Assert.IsLessThan(0, CompareBytes(encoded0, encoded1));
        Assert.IsLessThan(0, CompareBytes(encoded1, encodedMax));
    }

    [TestMethod]
    public void EncodeInt16_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode((short)42, GaldrFieldType.Int16);

        Assert.HasCount(3, encoded);
    }

    [TestMethod]
    public void EncodeUInt16_MaintainsSortOrder()
    {
        byte[] encoded0 = IndexKeyEncoder.Encode((ushort)0, GaldrFieldType.UInt16);
        byte[] encoded1000 = IndexKeyEncoder.Encode((ushort)1000, GaldrFieldType.UInt16);
        byte[] encodedMax = IndexKeyEncoder.Encode(ushort.MaxValue, GaldrFieldType.UInt16);

        Assert.IsLessThan(0, CompareBytes(encoded0, encoded1000));
        Assert.IsLessThan(0, CompareBytes(encoded1000, encodedMax));
    }

    [TestMethod]
    public void EncodeUInt16_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode((ushort)42, GaldrFieldType.UInt16);

        Assert.HasCount(3, encoded);
    }

    [TestMethod]
    public void EncodeUInt32_MaintainsSortOrder()
    {
        byte[] encoded0 = IndexKeyEncoder.Encode(0U, GaldrFieldType.UInt32);
        byte[] encoded1000 = IndexKeyEncoder.Encode(1000U, GaldrFieldType.UInt32);
        byte[] encodedMax = IndexKeyEncoder.Encode(uint.MaxValue, GaldrFieldType.UInt32);

        Assert.IsLessThan(0, CompareBytes(encoded0, encoded1000));
        Assert.IsLessThan(0, CompareBytes(encoded1000, encodedMax));
    }

    [TestMethod]
    public void EncodeUInt32_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(42U, GaldrFieldType.UInt32);

        Assert.HasCount(5, encoded);
    }

    [TestMethod]
    public void EncodeUInt64_MaintainsSortOrder()
    {
        byte[] encoded0 = IndexKeyEncoder.Encode(0UL, GaldrFieldType.UInt64);
        byte[] encoded1000 = IndexKeyEncoder.Encode(1000UL, GaldrFieldType.UInt64);
        byte[] encodedLarge = IndexKeyEncoder.Encode(10000000000UL, GaldrFieldType.UInt64);
        byte[] encodedMax = IndexKeyEncoder.Encode(ulong.MaxValue, GaldrFieldType.UInt64);

        Assert.IsLessThan(0, CompareBytes(encoded0, encoded1000));
        Assert.IsLessThan(0, CompareBytes(encoded1000, encodedLarge));
        Assert.IsLessThan(0, CompareBytes(encodedLarge, encodedMax));
    }

    [TestMethod]
    public void EncodeUInt64_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(42UL, GaldrFieldType.UInt64);

        Assert.HasCount(9, encoded);
    }

    [TestMethod]
    public void EncodeSingle_PositiveNumbers_MaintainsSortOrder()
    {
        byte[] encoded1 = IndexKeyEncoder.Encode(1.0f, GaldrFieldType.Single);
        byte[] encoded2 = IndexKeyEncoder.Encode(2.5f, GaldrFieldType.Single);
        byte[] encoded100 = IndexKeyEncoder.Encode(100.75f, GaldrFieldType.Single);

        Assert.IsLessThan(0, CompareBytes(encoded1, encoded2));
        Assert.IsLessThan(0, CompareBytes(encoded2, encoded100));
    }

    [TestMethod]
    public void EncodeSingle_NegativeNumbers_MaintainsSortOrder()
    {
        byte[] encodedNeg100 = IndexKeyEncoder.Encode(-100.5f, GaldrFieldType.Single);
        byte[] encodedNeg1 = IndexKeyEncoder.Encode(-1.0f, GaldrFieldType.Single);

        Assert.IsLessThan(0, CompareBytes(encodedNeg100, encodedNeg1));
    }

    [TestMethod]
    public void EncodeSingle_NegativeBeforePositive()
    {
        byte[] encodedNeg = IndexKeyEncoder.Encode(-1.0f, GaldrFieldType.Single);
        byte[] encoded0 = IndexKeyEncoder.Encode(0.0f, GaldrFieldType.Single);
        byte[] encodedPos = IndexKeyEncoder.Encode(1.0f, GaldrFieldType.Single);

        Assert.IsLessThan(0, CompareBytes(encodedNeg, encoded0));
        Assert.IsLessThan(0, CompareBytes(encoded0, encodedPos));
    }

    [TestMethod]
    public void EncodeSingle_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(3.14f, GaldrFieldType.Single);

        Assert.HasCount(5, encoded);
    }

    [TestMethod]
    public void EncodeChar_MaintainsSortOrder()
    {
        byte[] encodedA = IndexKeyEncoder.Encode('A', GaldrFieldType.Char);
        byte[] encodedB = IndexKeyEncoder.Encode('B', GaldrFieldType.Char);
        byte[] encodedZ = IndexKeyEncoder.Encode('Z', GaldrFieldType.Char);
        byte[] encodeda = IndexKeyEncoder.Encode('a', GaldrFieldType.Char);

        Assert.IsLessThan(0, CompareBytes(encodedA, encodedB));
        Assert.IsLessThan(0, CompareBytes(encodedB, encodedZ));
        Assert.IsLessThan(0, CompareBytes(encodedZ, encodeda));
    }

    [TestMethod]
    public void EncodeChar_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode('X', GaldrFieldType.Char);

        Assert.HasCount(3, encoded);
    }

    [TestMethod]
    public void EncodeTimeSpan_MaintainsSortOrder()
    {
        TimeSpan ts1 = TimeSpan.FromMinutes(30);
        TimeSpan ts2 = TimeSpan.FromHours(2);
        TimeSpan ts3 = TimeSpan.FromDays(1);

        byte[] encoded1 = IndexKeyEncoder.Encode(ts1, GaldrFieldType.TimeSpan);
        byte[] encoded2 = IndexKeyEncoder.Encode(ts2, GaldrFieldType.TimeSpan);
        byte[] encoded3 = IndexKeyEncoder.Encode(ts3, GaldrFieldType.TimeSpan);

        Assert.IsLessThan(0, CompareBytes(encoded1, encoded2));
        Assert.IsLessThan(0, CompareBytes(encoded2, encoded3));
    }

    [TestMethod]
    public void EncodeTimeSpan_NegativeBeforePositive()
    {
        TimeSpan tsNeg = TimeSpan.FromHours(-1);
        TimeSpan tsZero = TimeSpan.Zero;
        TimeSpan tsPos = TimeSpan.FromHours(1);

        byte[] encodedNeg = IndexKeyEncoder.Encode(tsNeg, GaldrFieldType.TimeSpan);
        byte[] encodedZero = IndexKeyEncoder.Encode(tsZero, GaldrFieldType.TimeSpan);
        byte[] encodedPos = IndexKeyEncoder.Encode(tsPos, GaldrFieldType.TimeSpan);

        Assert.IsLessThan(0, CompareBytes(encodedNeg, encodedZero));
        Assert.IsLessThan(0, CompareBytes(encodedZero, encodedPos));
    }

    [TestMethod]
    public void EncodeTimeSpan_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(TimeSpan.FromHours(1), GaldrFieldType.TimeSpan);

        Assert.HasCount(9, encoded);
    }

    [TestMethod]
    public void EncodeDateOnly_MaintainsSortOrder()
    {
        DateOnly date1 = new DateOnly(2020, 1, 1);
        DateOnly date2 = new DateOnly(2022, 6, 15);
        DateOnly date3 = new DateOnly(2025, 12, 31);

        byte[] encoded1 = IndexKeyEncoder.Encode(date1, GaldrFieldType.DateOnly);
        byte[] encoded2 = IndexKeyEncoder.Encode(date2, GaldrFieldType.DateOnly);
        byte[] encoded3 = IndexKeyEncoder.Encode(date3, GaldrFieldType.DateOnly);

        Assert.IsLessThan(0, CompareBytes(encoded1, encoded2));
        Assert.IsLessThan(0, CompareBytes(encoded2, encoded3));
    }

    [TestMethod]
    public void EncodeDateOnly_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(DateOnly.FromDateTime(DateTime.Today), GaldrFieldType.DateOnly);

        Assert.HasCount(5, encoded);
    }

    [TestMethod]
    public void EncodeTimeOnly_MaintainsSortOrder()
    {
        TimeOnly time1 = new TimeOnly(8, 0, 0);
        TimeOnly time2 = new TimeOnly(12, 30, 0);
        TimeOnly time3 = new TimeOnly(23, 59, 59);

        byte[] encoded1 = IndexKeyEncoder.Encode(time1, GaldrFieldType.TimeOnly);
        byte[] encoded2 = IndexKeyEncoder.Encode(time2, GaldrFieldType.TimeOnly);
        byte[] encoded3 = IndexKeyEncoder.Encode(time3, GaldrFieldType.TimeOnly);

        Assert.IsLessThan(0, CompareBytes(encoded1, encoded2));
        Assert.IsLessThan(0, CompareBytes(encoded2, encoded3));
    }

    [TestMethod]
    public void EncodeTimeOnly_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(new TimeOnly(12, 0, 0), GaldrFieldType.TimeOnly);

        Assert.HasCount(9, encoded);
    }

    private static int CompareBytes(byte[] a, byte[] b)
    {
        int minLength = Math.Min(a.Length, b.Length);
        for (int i = 0; i < minLength; i++)
        {
            int cmp = a[i].CompareTo(b[i]);
            if (cmp != 0)
            {
                return cmp;
            }
        }

        return a.Length.CompareTo(b.Length);
    }
}
