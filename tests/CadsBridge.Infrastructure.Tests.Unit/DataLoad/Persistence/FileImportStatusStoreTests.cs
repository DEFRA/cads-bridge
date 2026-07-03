using System.Net;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.DataLoad.Persistence;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Persistence;

public class FileImportStatusStoreTests
{
    private readonly Mock<IFileImportStatusApiService> _apiService = new();
    private readonly Mock<ILogger<FileImportStatusStore>> _logger = new();

    private FileImportStatusStore CreateSut() => new(_apiService.Object, _logger.Object);

    public class InitiateTests : FileImportStatusStoreTests
    {
        [Fact]
        public async Task Initiate_ReturnsId_WhenApiServiceSucceeds()
        {
            _apiService
                .Setup(x => x.Create("file.csv", 100L, It.IsAny<CancellationToken>()))
                .ReturnsAsync(7L);

            var result = await CreateSut().Initiate("file.csv", 100L, TestContext.Current.CancellationToken);

            result.Should().Be(7L);
        }

        [Fact]
        public async Task Initiate_ReturnsZero_WhenApiServiceThrowsConflict()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new HttpRequestException("conflict", null, HttpStatusCode.Conflict));

            var result = await CreateSut().Initiate("file.csv", 100L, TestContext.Current.CancellationToken);

            result.Should().Be(0L);
        }

        [Theory]
        [InlineData(HttpStatusCode.BadRequest)]
        [InlineData(HttpStatusCode.InternalServerError)]
        [InlineData(HttpStatusCode.NotFound)]
        public async Task Initiate_Rethrows_WhenApiServiceThrowsNonConflictHttpRequestException(HttpStatusCode statusCode)
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new HttpRequestException("error", null, statusCode));

            var act = async () => await CreateSut().Initiate("file.csv", 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<HttpRequestException>();
        }

        [Fact]
        public async Task Initiate_Rethrows_WhenHttpRequestExceptionHasNoStatusCode()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new HttpRequestException("error"));

            var act = async () => await CreateSut().Initiate("file.csv", 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<HttpRequestException>();
        }
    }
}