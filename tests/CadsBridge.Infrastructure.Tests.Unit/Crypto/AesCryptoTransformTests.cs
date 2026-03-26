using FluentAssertions;
using CadsBridge.Infrastructure.Crypto;
using System.Text;
using Xunit;

namespace CadsBridge.Infrastructure.Tests.Unit.Crypto;

public class AesCryptoTransformTests
{
    private const string TestPassword = "2025-08-05_ADDRESSES_CT_01628_DELTA_PROD_UKV_CTSM";
    private const string TestSalt = "Jr8Lm2PXzd7qNbVyWutRfGBxhkHTpE";
    
    private readonly AesCryptoTransform _cryptoTransform;
    private readonly List<string> _tempFiles = new();
    private readonly string _tempDir;

    public AesCryptoTransformTests()
    {
        _cryptoTransform = new AesCryptoTransform();
        _tempDir = Path.Combine(Path.GetTempPath(), "AesCryptoTransformTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_tempDir);
    }
    
    [Fact]
    public async Task EncryptStreamAsync_WithByteArraySalt_ShouldEncryptStreamSuccessfully()
    {
        // Arrange
        var inputData = "test stream data for encryption";
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(inputData));
        using var outputStream = new MemoryStream();
        var progressReports = new List<(int percentage, string status)>();

        // Act
        await _cryptoTransform.EncryptStreamAsync(
            inputStream,
            outputStream,
            TestPassword,
            TestSalt,
            inputStream.Length,
            (percentage, status) => progressReports.Add((percentage, status)));

        // Assert
        outputStream.Length.Should().BeGreaterThan(0);
        outputStream.ToArray().Should().NotEqual(Encoding.UTF8.GetBytes(inputData));
        progressReports.Should().NotBeEmpty();
    }
    
        [Fact]
    public async Task EncryptStreamAsync_WithStringSalt_ShouldEncryptStreamSuccessfully()
    {
        // Arrange
        var inputData = "test stream data for encryption";
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(inputData));
        using var outputStream = new MemoryStream();

        // Act
        await _cryptoTransform.EncryptStreamAsync(inputStream, outputStream, TestPassword, TestSalt);

        // Assert
        outputStream.Length.Should().BeGreaterThan(0);
        outputStream.ToArray().Should().NotEqual(Encoding.UTF8.GetBytes(inputData));
    }

    [Fact]
    public async Task DecryptStreamAsync_WithByteArraySalt_ShouldDecryptStreamSuccessfully()
    {
        // Arrange
        var originalData = "test stream data for round-trip encryption/decryption";
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(originalData));
        using var encryptedStream = new MemoryStream();
        using var decryptedStream = new MemoryStream();

        // Act
        await _cryptoTransform.EncryptStreamAsync(inputStream, encryptedStream, TestPassword, TestSalt);
        encryptedStream.Position = 0;
        await _cryptoTransform.DecryptStreamAsync(encryptedStream, decryptedStream, TestPassword, TestSalt);

        // Assert
        var decryptedData = Encoding.UTF8.GetString(decryptedStream.ToArray());
        decryptedData.Should().Be(originalData);
    }

    [Fact]
    public async Task DecryptStreamAsync_WithStringSalt_ShouldDecryptStreamSuccessfully()
    {
        // Arrange
        var originalData = "test stream data for round-trip encryption/decryption";
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(originalData));
        using var encryptedStream = new MemoryStream();
        using var decryptedStream = new MemoryStream();

        // Act
        await _cryptoTransform.EncryptStreamAsync(inputStream, encryptedStream, TestPassword, TestSalt);
        encryptedStream.Position = 0;
        await _cryptoTransform.DecryptStreamAsync(encryptedStream, decryptedStream, TestPassword, TestSalt);

        // Assert
        var decryptedData = Encoding.UTF8.GetString(decryptedStream.ToArray());
        decryptedData.Should().Be(originalData);
    }
    
        [Fact]
    public async Task EncryptDecrypt_WithEmptySalt_ShouldWorkCorrectly()
    {
        // Arrange
        var originalData = "test data with empty salt";
        var emptySalt = string.Empty;
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(originalData));
        using var encryptedStream = new MemoryStream();
        using var decryptedStream = new MemoryStream();

        // Act
        await _cryptoTransform.EncryptStreamAsync(inputStream, encryptedStream, TestPassword, emptySalt);
        encryptedStream.Position = 0;
        await _cryptoTransform.DecryptStreamAsync(encryptedStream, decryptedStream, TestPassword, emptySalt);

        // Assert
        var decryptedData = Encoding.UTF8.GetString(decryptedStream.ToArray());
        decryptedData.Should().Be(originalData);
    }
    
    [Fact]
    public async Task EncryptDecrypt_WithNullEmptyStringSalt_ShouldWorkCorrectly()
    {
        // Arrange
        var originalData = "test data with null/empty string salt";
        using var inputStream1 = new MemoryStream(Encoding.UTF8.GetBytes(originalData));
        using var encryptedStream1 = new MemoryStream();
        using var decryptedStream1 = new MemoryStream();
        using var inputStream2 = new MemoryStream(Encoding.UTF8.GetBytes(originalData));
        using var encryptedStream2 = new MemoryStream();
        using var decryptedStream2 = new MemoryStream();

        // Act - Test with null string salt
        await _cryptoTransform.EncryptStreamAsync(inputStream1, encryptedStream1, TestPassword, (string)null!);
        encryptedStream1.Position = 0;
        await _cryptoTransform.DecryptStreamAsync(encryptedStream1, decryptedStream1, TestPassword, (string)null!);

        // Act - Test with empty string salt
        await _cryptoTransform.EncryptStreamAsync(inputStream2, encryptedStream2, TestPassword, "");
        encryptedStream2.Position = 0;
        await _cryptoTransform.DecryptStreamAsync(encryptedStream2, decryptedStream2, TestPassword, "");

        // Assert
        var decryptedData1 = Encoding.UTF8.GetString(decryptedStream1.ToArray());
        var decryptedData2 = Encoding.UTF8.GetString(decryptedStream2.ToArray());
        decryptedData1.Should().Be(originalData);
        decryptedData2.Should().Be(originalData);
    }

    [Fact]
    public async Task EncryptStreamAsync_WithCancellation_ShouldThrowOperationCanceledException()
    {
        // Arrange
        var inputData = new byte[1024 * 1024]; // 1MB of data
        new Random().NextBytes(inputData);
        using var inputStream = new MemoryStream(inputData);
        using var outputStream = new MemoryStream();
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            _cryptoTransform.EncryptStreamAsync(inputStream, outputStream, TestPassword, TestSalt,
                cancellationToken: cts.Token));

        exception.Should().NotBeNull();
    }

    [Fact]
    public async Task DecryptStreamAsync_WithCancellation_ShouldThrowOperationCanceledException()
    {
        // Arrange
        var inputData = new byte[1024 * 1024]; // 1MB of data
        new Random().NextBytes(inputData);
        using var inputStream = new MemoryStream(inputData);
        using var outputStream = new MemoryStream();
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            _cryptoTransform.DecryptStreamAsync(inputStream, outputStream, TestPassword, TestSalt,
                cancellationToken: cts.Token));

        exception.Should().NotBeNull();
    }
    [Fact]
    public async Task StreamProcessing_WithoutTotalBytes_ShouldReportProgressWithoutPercentage()
    {
        // Arrange
        var inputData = "test data without total bytes";
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(inputData));
        using var outputStream = new MemoryStream();
        var progressReports = new List<(int percentage, string status)>();

        // Act
        await _cryptoTransform.EncryptStreamAsync(
            inputStream,
            outputStream,
            TestPassword,
            TestSalt,
            totalBytes: null, // No total bytes provided
            (percentage, status) => progressReports.Add((percentage, status)));

        // Assert
        progressReports.Should().NotBeEmpty();
        progressReports.Should().Contain(r => r.percentage == 0 && r.status.Contains("Encrypting started"));
        progressReports.Should().Contain(r => r.percentage == 100 && r.status.Contains("Encrypting completed"));
        progressReports.Should().Contain(r => r.percentage == 0 && r.status.Contains("processed") && !r.status.Contains("%"));
    }
}