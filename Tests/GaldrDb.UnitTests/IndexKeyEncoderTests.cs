using System;
using GaldrDbCore.Query;
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

        Assert.HasCount(4, encoded);
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

        Assert.HasCount(8, encoded);
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

        Assert.HasCount(8, encoded);
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

        Assert.IsEmpty(encoded);
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

        Assert.HasCount(1, encoded);
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

        Assert.HasCount(8, encoded);
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

        Assert.HasCount(16, encoded);
    }

    [TestMethod]
    public void EncodeGuid_ReturnsCorrectLength()
    {
        byte[] encoded = IndexKeyEncoder.Encode(Guid.NewGuid(), GaldrFieldType.Guid);

        Assert.HasCount(16, encoded);
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

        Assert.HasCount(16, encoded);
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
