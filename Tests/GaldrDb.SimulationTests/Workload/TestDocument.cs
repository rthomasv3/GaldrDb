using System;
using System.Security.Cryptography;
using System.Text;
using GaldrDb.SimulationTests.Core;
using GaldrJson;

namespace GaldrDb.SimulationTests.Workload;

[GaldrDbCollection]
public class TestDocument
{
    public int Id { get; set; }
    public string Name { get; set; }
    public int Counter { get; set; }
    public byte[] Payload { get; set; }
    public DateTime CreatedAt { get; set; }
    public string Category { get; set; }

    public static TestDocument Generate(SimulationRandom rng, int targetSize)
    {
        string[] categories = { "Alpha", "Beta", "Gamma", "Delta", "Epsilon" };

        TestDocument doc = new TestDocument
        {
            Id = 0, // Will be assigned by database
            Name = GenerateRandomString(rng, 10, 50),
            Counter = rng.Next(0, 10000),
            CreatedAt = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(rng.Next(0, 365)),
            Category = categories[rng.Next(categories.Length)]
        };

        // Calculate how much payload we need to reach target size
        // Rough estimate: Id (4) + Name (~30) + Counter (4) + CreatedAt (8) + Category (~5) = ~51 base
        int baseSize = 51 + (doc.Name?.Length ?? 0) + (doc.Category?.Length ?? 0);
        int payloadSize = Math.Max(0, targetSize - baseSize);

        if (payloadSize > 0)
        {
            doc.Payload = new byte[payloadSize];
            rng.NextBytes(doc.Payload);
        }
        else
        {
            doc.Payload = Array.Empty<byte>();
        }

        return doc;
    }

    public byte[] ComputeHash()
    {
        byte[] result;

        using (SHA256 sha256 = SHA256.Create())
        {
            // Create a deterministic byte representation
            StringBuilder sb = new StringBuilder();
            sb.Append(Id);
            sb.Append('|');
            sb.Append(Name ?? "");
            sb.Append('|');
            sb.Append(Counter);
            sb.Append('|');
            sb.Append(CreatedAt.Ticks);
            sb.Append('|');
            sb.Append(Category ?? "");
            sb.Append('|');

            byte[] baseBytes = Encoding.UTF8.GetBytes(sb.ToString());

            // Combine with payload
            byte[] combined;
            if (Payload != null && Payload.Length > 0)
            {
                combined = new byte[baseBytes.Length + Payload.Length];
                Buffer.BlockCopy(baseBytes, 0, combined, 0, baseBytes.Length);
                Buffer.BlockCopy(Payload, 0, combined, baseBytes.Length, Payload.Length);
            }
            else
            {
                combined = baseBytes;
            }

            result = sha256.ComputeHash(combined);
        }

        return result;
    }

    public TestDocument Clone()
    {
        return new TestDocument
        {
            Id = Id,
            Name = Name,
            Counter = Counter,
            Payload = Payload != null ? (byte[])Payload.Clone() : null,
            CreatedAt = CreatedAt,
            Category = Category
        };
    }

    private static string GenerateRandomString(SimulationRandom rng, int minLength, int maxLength)
    {
        int length = rng.Next(minLength, maxLength + 1);
        char[] chars = new char[length];

        for (int i = 0; i < length; i++)
        {
            // Generate alphanumeric characters
            int charType = rng.Next(3);
            if (charType == 0)
            {
                chars[i] = (char)('A' + rng.Next(26));
            }
            else if (charType == 1)
            {
                chars[i] = (char)('a' + rng.Next(26));
            }
            else
            {
                chars[i] = (char)('0' + rng.Next(10));
            }
        }

        return new string(chars);
    }
}
