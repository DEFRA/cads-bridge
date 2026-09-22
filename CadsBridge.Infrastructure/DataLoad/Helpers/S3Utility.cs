namespace CadsBridge.Infrastructure.DataLoad.Helpers;

public class S3Utility
{
    private const long MinPartitionSize = 5L * 1024 * 1024; // 5 MB (S3 minimum)

    public static long CalculateOptimalPartSize(long fileSizeBytes)
    {
        // AWS recommendation:
        // For files< 100 MB: Single PUT(no multipart needed).
        // For files 100 MB – 5 GB: Multipart with 8–64 MB parts.
        // For files > 5 GB: Larger part sizes(e.g., 64–128 MB) to reduce part count.

        if (fileSizeBytes <= 0)
            throw new ArgumentException("File size must be greater than zero.", nameof(fileSizeBytes));

        const long RecommendedMin = 8L * 1024 * 1024; // 8 MB (better performance)
        const long RecommendedMax = 128L * 1024 * 1024; // 128 MB (avoid huge retries)
        const int MaxParts = 10_000;

        // Calculate minimum size to not exceed 10,000 parts
        var requiredPartSize = (long)Math.Ceiling((double)fileSizeBytes / MaxParts);

        // Ensure part size is at least the S3 minimum
        var optimalPartSize = Math.Max(MinPartitionSize, requiredPartSize);

        // Apply recommended lower bound for performance
        if (optimalPartSize < RecommendedMin)
            optimalPartSize = RecommendedMin;

        // Cap at recommended max unless file is extremely large
        if (optimalPartSize > RecommendedMax && fileSizeBytes < (RecommendedMax * MaxParts))
            optimalPartSize = RecommendedMax;

        return optimalPartSize;
    }
}