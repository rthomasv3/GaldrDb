using System;
using System.Security.Cryptography;
using System.Text;
using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDbConsole.StressTest;

[GaldrDbCollection]
public class StressTestDocument
{
    public int Id { get; set; }

    [GaldrDbIndex]
    public string Name { get; set; }

    public int Counter { get; set; }

    public string Category { get; set; }

    public DateTime CreatedAt { get; set; }

    public DateTime UpdatedAt { get; set; }

    public int WorkerId { get; set; }

    public int Version { get; set; }

    public string Payload { get; set; }

    public byte[] ComputeHash()
    {
        StringBuilder sb = new StringBuilder();
        sb.Append(Id);
        sb.Append('|');
        sb.Append(Name ?? string.Empty);
        sb.Append('|');
        sb.Append(Counter);
        sb.Append('|');
        sb.Append(Category ?? string.Empty);
        sb.Append('|');
        sb.Append(CreatedAt.Ticks);
        sb.Append('|');
        sb.Append(UpdatedAt.Ticks);
        sb.Append('|');
        sb.Append(WorkerId);
        sb.Append('|');
        sb.Append(Version);
        sb.Append('|');
        sb.Append(Payload ?? string.Empty);

        byte[] data = Encoding.UTF8.GetBytes(sb.ToString());
        return SHA256.HashData(data);
    }

    public static StressTestDocument Generate(Random rng, int workerId)
    {
        DateTime now = DateTime.UtcNow;

        StressTestDocument doc = new StressTestDocument
        {
            Name = $"Doc_{Guid.NewGuid():N}",
            Counter = rng.Next(1, 10000),
            Category = CATEGORIES[rng.Next(CATEGORIES.Length)],
            CreatedAt = now,
            UpdatedAt = now,
            WorkerId = workerId,
            Version = 1,
            Payload = GeneratePayload(rng, MIN_PAYLOAD_SIZE, MAX_PAYLOAD_SIZE)
        };

        return doc;
    }

    public StressTestDocument CreateUpdated(Random rng, int workerId)
    {
        DateTime now = DateTime.UtcNow;

        StressTestDocument updated = new StressTestDocument
        {
            Id = Id,
            Name = Name,
            Counter = Counter + 1,
            Category = CATEGORIES[rng.Next(CATEGORIES.Length)],
            CreatedAt = CreatedAt,
            UpdatedAt = now,
            WorkerId = workerId,
            Version = Version + 1,
            Payload = GeneratePayload(rng, MIN_PAYLOAD_SIZE, MAX_PAYLOAD_SIZE)
        };

        return updated;
    }

    private static string GeneratePayload(Random rng, int minSize, int maxSize)
    {
        int size = rng.Next(minSize, maxSize + 1);
        StringBuilder sb = new StringBuilder(size);

        for (int i = 0; i < size; i++)
        {
            sb.Append(PAYLOAD_CHARS[rng.Next(PAYLOAD_CHARS.Length)]);
        }

        return sb.ToString();
    }

    private static readonly string[] CATEGORIES = { "Alpha", "Beta", "Gamma", "Delta", "Epsilon" };
    private static readonly string PAYLOAD_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private const int MIN_PAYLOAD_SIZE = 50;
    private const int MAX_PAYLOAD_SIZE = 500;
}
