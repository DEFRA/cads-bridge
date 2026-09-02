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
    private const string DestinationPrefix = "import/cts/bulk";
    private readonly Mock<IFileImportApiService> _apiService = new();
    private readonly Mock<ILogger<FileImportStore>> _logger = new Mock<ILogger<FileImportStore>>().EnableAllLogLevels();
    private FileImportStore CreateSut() => new(_apiService.Object, _logger.Object);

    public class InitiateTests : FileImportStatusStoreTests
    {
        [Fact]
        public async Task Initiate_ReturnsId_WhenApiServiceSucceeds()
        {
            _apiService
                .Setup(x => x.Create("file.csv", DestinationPrefix, 100L, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImport
                {
                    Id = 7L,
                    FileName = "file.csv",
                    DestinationTableName = "some_table",
                    ImportStatus = FileImportStatus.Pending,
                    FailedAttempts = 0
                });

            var result = await CreateSut().CreateAsync("file.csv", DestinationPrefix, 100L, TestContext.Current.CancellationToken);

            result.Should().Be(7L);
        }

        [Fact]
        public async Task Creates_ThrowsExceptions_WhenApiSucceedsWithUknownDestinationTable()
        {
            _apiService
                .Setup(x => x.Create("file.csv", DestinationPrefix, 100L, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImport
                {
                    Id = 7L,
                    FileName = "file.csv",
                    DestinationTableName = "UNKNOWN",
                    ImportStatus = FileImportStatus.Pending,
                    FailedAttempts = 0
                });

            var act = async () => await CreateSut().CreateAsync("file.csv", DestinationPrefix, 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<BusinessRuleValidationException>();
        }

        [Fact]
        public async Task Initiate_ResetsExistingRecordAndReturnsItsId_WhenCreateThrowsConflict()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ConflictException("file import already exists"));
            _apiService
                .Setup(x => x.GetByFileName("file.csv", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImportDto { Id = 55, FileName = "file.csv" });

            var result = await CreateSut().CreateAsync("file.csv", DestinationPrefix, 100L, TestContext.Current.CancellationToken);

            result.Should().Be(55L);
            _apiService.Verify(x => x.MarkReset(55L, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Initiate_ResetsExistingRecordAndReturnsItsId_WhenConflictAndStatusFailedLessThan3Times()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ConflictException("file import already exists"));
            _apiService
                .Setup(x => x.GetByFileName("file.csv", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImportDto(1) { Id = 55, FileName = "file.csv", ImportStatus = FileImportStatus.Failed });

            var result = await CreateSut().CreateAsync("file.csv", DestinationPrefix, 100L, TestContext.Current.CancellationToken);

            result.Should().Be(55L);
            _apiService.Verify(x => x.MarkReset(55L, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Initiate_ThrowsInvalidOperationException_WhenConflictButStatusFailed3TimesOrMore()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ConflictException("file import already exists"));
            _apiService
                .Setup(x => x.GetByFileName("file.csv", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImportDto(3) { Id = 55, FileName = "file.csv", ImportStatus = FileImportStatus.Failed });

            var act = async () => await CreateSut().CreateAsync("file.csv", DestinationPrefix, 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<InvalidOperationException>();
            _apiService.Verify(x => x.MarkReset(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task Initiate_ThrowsInvalidOperationException_WhenConflictButStatusCompleted()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ConflictException("file import already exists"));
            _apiService
                .Setup(x => x.GetByFileName("file.csv", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileImportDto { Id = 55, FileName = "file.csv", ImportStatus = FileImportStatus.Completed });

            var act = async () => await CreateSut().CreateAsync("file.csv", DestinationPrefix, 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<InvalidOperationException>();
            _apiService.Verify(x => x.MarkReset(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task Initiate_ThrowsNotFound_WhenConflictButExistingRecordNotFound()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ConflictException("file import already exists"));
            _apiService
                .Setup(x => x.GetByFileName(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileImportDto?)null);

            var act = async () => await CreateSut().CreateAsync("file.csv", DestinationPrefix, 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NotFoundException>();
            _apiService.Verify(x => x.MarkReset(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task Initiate_Propagates_WhenCreateThrowsNonConflict()
        {
            _apiService
                .Setup(x => x.Create(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new NonRetryableException("permanent failure"));

            var act = async () => await CreateSut().CreateAsync("file.csv", DestinationPrefix, 100L, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NonRetryableException>();
            _apiService.Verify(x => x.GetByFileName(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }
    }

    public class MarkStatusTests : FileImportStatusStoreTests
    {
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