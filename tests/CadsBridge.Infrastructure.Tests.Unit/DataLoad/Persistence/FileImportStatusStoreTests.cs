using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.DataLoad.Persistence;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;

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
        public async Task Initiate_ResetsExistingRecordAndReturnsItsId_WhenCreateThrowsConflict()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ConflictException("file import already exists"));
            _apiService
                .Setup(x => x.GetByFileName("file.csv", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImportStatusDto { Id = 55, FileName = "file.csv" });

            var result = await CreateSut().Initiate("file.csv", 100L, TestContext.Current.CancellationToken);

            result.Should().Be(55L);
            _apiService.Verify(x => x.MarkReset(55L, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Initiate_ThrowsNotFound_WhenConflictButExistingRecordNotFound()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ConflictException("file import already exists"));
            _apiService
                .Setup(x => x.GetByFileName(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileImportStatusDto?)null);

            var act = async () => await CreateSut().Initiate("file.csv", 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NotFoundException>();
            _apiService.Verify(x => x.MarkReset(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task Initiate_Propagates_WhenCreateThrowsNonConflict()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new NonRetryableException("permanent failure"));

            var act = async () => await CreateSut().Initiate("file.csv", 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NonRetryableException>();
            _apiService.Verify(x => x.GetByFileName(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }
    }

    public class MarkStatusTests : FileImportStatusStoreTests
    {
        [Fact]
        public async Task MarkInProgress_CallsApiService_WithImportingStatus()
        {
            await CreateSut().MarkInProgress(5L, TestContext.Current.CancellationToken);

            _apiService.Verify(x =>
                x.MarkStatus(5L, FileImportStatus.Importing, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task MarkSucceeded_CallsApiService_WithCompletedStatus()
        {
            await CreateSut().MarkSucceeded(5L, TestContext.Current.CancellationToken);

            _apiService.Verify(x =>
                x.MarkStatus(5L, FileImportStatus.Completed, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task MarkFailed_CallsApiService_WithFailedStatus()
        {
            await CreateSut().MarkFailed(5L, TestContext.Current.CancellationToken);

            _apiService.Verify(x =>
                x.MarkStatus(5L, FileImportStatus.Failed, It.IsAny<CancellationToken>()), Times.Once);
        }
    }
}