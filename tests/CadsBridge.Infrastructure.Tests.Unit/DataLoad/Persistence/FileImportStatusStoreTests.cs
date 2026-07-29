using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Core.ApiClients;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.DataLoad.Persistence;
using CadsBridge.Testing.Support.Utilities.Logging;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.DataLoad.Persistence;

public class FileImportStatusStoreTests
{
    private readonly Mock<IFileImportApiService> _apiService = new();
    private readonly Mock<ILogger<FileImportStore>> _logger = new Mock<ILogger<FileImportStore>>().EnableAllLogLevels();
    private FileImportStore CreateSut() => new(_apiService.Object, _logger.Object);

    public class InitiateTests : FileImportStatusStoreTests
    {
        [Fact]
        public async Task Initiate_ReturnsId_WhenApiServiceSucceeds()
        {
            _apiService
                .Setup(x => x.Create("file.csv", 100L, It.IsAny<CancellationToken>()))
                .ReturnsAsync(7L);

            var result = await CreateSut().CreateAsync("file.csv", 100L, TestContext.Current.CancellationToken);

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
                .ReturnsAsync(new FileImportDto { Id = 55, FileName = "file.csv" });

            var result = await CreateSut().CreateAsync("file.csv", 100L, TestContext.Current.CancellationToken);

            result.Should().Be(55L);
            _apiService.Verify(x => x.MarkReset(55L, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Initiate_ResetsExistingRecordAndReturnsItsId_WhenConflictAndStatusFailedLessThan3Times()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ConflictException("file import already exists"));
            _apiService
                .Setup(x => x.GetByFileName("file.csv", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImportDto(1) { Id = 55, FileName = "file.csv", ImportStatus = FileImportStatus.Failed });

            var result = await CreateSut().CreateAsync("file.csv", 100L, TestContext.Current.CancellationToken);

            result.Should().Be(55L);
            _apiService.Verify(x => x.MarkReset(55L, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Initiate_ThrowsInvalidOperationException_WhenConflictButStatusFailed3TimesOrMore()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ConflictException("file import already exists"));
            _apiService
                .Setup(x => x.GetByFileName("file.csv", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImportDto(3) { Id = 55, FileName = "file.csv", ImportStatus = FileImportStatus.Failed });

            var act = async () => await CreateSut().CreateAsync("file.csv", 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<InvalidOperationException>();
            _apiService.Verify(x => x.MarkReset(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task Initiate_ThrowsInvalidOperationException_WhenConflictButStatusCompleted()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ConflictException("file import already exists"));
            _apiService
                .Setup(x => x.GetByFileName("file.csv", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImportDto { Id = 55, FileName = "file.csv", ImportStatus = FileImportStatus.Completed });

            var act = async () => await CreateSut().CreateAsync("file.csv", 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<InvalidOperationException>();
            _apiService.Verify(x => x.MarkReset(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task Initiate_ThrowsNotFound_WhenConflictButExistingRecordNotFound()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ConflictException("file import already exists"));
            _apiService
                .Setup(x => x.GetByFileName(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileImportDto?)null);

            var act = async () => await CreateSut().CreateAsync("file.csv", 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NotFoundException>();
            _apiService.Verify(x => x.MarkReset(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task Initiate_Propagates_WhenCreateThrowsNonConflict()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new NonRetryableException("permanent failure"));

            var act = async () => await CreateSut().CreateAsync("file.csv", 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NonRetryableException>();
            _apiService.Verify(x => x.GetByFileName(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }
    }

    public class MarkStatusTests : FileImportStatusStoreTests
    {
        [Fact]
        public async Task MarkTransferred_CallsApiService_WithTransferredStatus()
        {
            await CreateSut().MarkTransferredAsync(5L, TestContext.Current.CancellationToken);

            _apiService.Verify(x =>
                x.MarkStatus(5L, FileImportStatus.Transferred, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task MarkSplit_CallsApiService_WithSplitStatus()
        {
            await CreateSut().MarkSplitAsync(5L, TestContext.Current.CancellationToken);

            _apiService.Verify(x =>
                x.MarkStatus(5L, FileImportStatus.Split, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task MarkCompleted_CallsApiService_WithCompletedStatus()
        {
            await CreateSut().MarkCompletedAsync(5L, TestContext.Current.CancellationToken);

            _apiService.Verify(x =>
                x.MarkStatus(5L, FileImportStatus.Completed, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task MarkFailed_CallsApiService_WithFailedStatus()
        {
            var reason = "Import failed for file.csv";
            await CreateSut().MarkFailedAsync(5L, reason, TestContext.Current.CancellationToken);

            _apiService.Verify(x =>
                x.MarkFailed(5L, reason, It.IsAny<CancellationToken>()), Times.Once);
        }
    }
}