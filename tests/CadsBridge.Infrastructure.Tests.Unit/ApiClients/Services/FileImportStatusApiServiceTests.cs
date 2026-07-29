using CadsBridge.Core.ApiClients;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.ApiClients.Services;
using CadsBridge.Testing.Support.Utilities.Http;
using CadsBridge.Testing.Support.Utilities.Logging;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using System.Net.Http.Json;

namespace CadsBridge.Infrastructure.Tests.Unit.ApiClients.Services;

public class FileImportStatusApiServiceTests
{
    private readonly Mock<IHttpClientFactory> _httpClientFactory = new();
    private readonly Mock<ILogger<FileImportApiService>> _logger = new();

    private FileImportApiService CreateSut(HttpMessageHandler handler)
    {
        // Enable all log levels so the IsEnabled-guarded logging branches are exercised.
        _logger.EnableAllLogLevels();
        var client = new HttpClient(handler) { BaseAddress = new Uri("http://test-api") };
        _httpClientFactory.Setup(x => x.CreateClient(nameof(ApiClientNames.CdsApi))).Returns(client);
        return new FileImportApiService(_httpClientFactory.Object, _logger.Object);
    }

    public class GetByFileNameTests : FileImportStatusApiServiceTests
    {
        [Fact]
        public async Task GetByFileName_ReturnsDto_WhenResponseIsSuccessful()
        {
            var dto = new FileImportDto { Id = 1, FileName = "file.csv", TotalRowsToProcess = 100 };
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(dto)
            });

            var result = await CreateSut(handler).GetByFileName("file.csv", TestContext.Current.CancellationToken);

            result.Should().NotBeNull();
            result.Id.Should().Be(1);
            result.FileName.Should().Be("file.csv");
        }

        [Fact]
        public async Task GetByFileName_SendsRequest_ToExpectedUrl()
        {
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(new FileImportDto())
            });

            await CreateSut(handler).GetByFileName("my file.csv", TestContext.Current.CancellationToken);

            handler.Requests.Should().ContainSingle();
            handler.Requests[0].RequestUri!.PathAndQuery
                .Should().Be("/api/v1/systemadmin/fileimports/search?fileName=my%20file.csv");
        }

        [Theory]
        [InlineData(HttpStatusCode.InternalServerError)]
        [InlineData(HttpStatusCode.RequestTimeout)]
        public async Task GetByFileName_ThrowsRetryable_OnTransientFailure(HttpStatusCode statusCode)
        {
            var handler = new StubHttpMessageHandler(statusCode);

            var act = async () => await CreateSut(handler)
                .GetByFileName("file.csv", TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<RetryableException>();
        }

        [Theory]
        [InlineData(HttpStatusCode.NotFound)]
        [InlineData(HttpStatusCode.BadRequest)]
        public async Task GetByFileName_ThrowsNonRetryable_OnPermanentFailure(HttpStatusCode statusCode)
        {
            var handler = new StubHttpMessageHandler(statusCode);

            var act = async () => await CreateSut(handler)
                .GetByFileName("file.csv", TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NonRetryableException>();
        }
    }

    public class GetByFileNameIfExistsTests : FileImportStatusApiServiceTests
    {
        [Fact]
        public async Task GetByFileNameIfExists_ReturnsDto_WhenResponseIsSuccessful()
        {
            var dto = new FileImportDto { Id = 1, FileName = "file.csv", TotalRowsToProcess = 100 };
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(dto)
            });

            var result = await CreateSut(handler).GetByFileNameIfExists("file.csv", TestContext.Current.CancellationToken);

            result.Should().NotBeNull();
            result.Id.Should().Be(1);
            result.FileName.Should().Be("file.csv");
        }

        [Fact]
        public async Task GetByFileNameIfExists_ReturnsNull_WhenResponseIsNotFound()
        {
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.NotFound));

            var result = await CreateSut(handler).GetByFileNameIfExists("file.csv", TestContext.Current.CancellationToken);

            result.Should().BeNull();
        }

        [Theory]
        [InlineData(HttpStatusCode.InternalServerError)]
        [InlineData(HttpStatusCode.RequestTimeout)]
        public async Task GetByFileNameIfExists_ThrowsRetryable_OnTransientFailure(HttpStatusCode statusCode)
        {
            var handler = new StubHttpMessageHandler(statusCode);

            var act = async () => await CreateSut(handler)
                .GetByFileNameIfExists("file.csv", TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<RetryableException>();
        }

        [Fact]
        public async Task GetByFileNameIfExists_ThrowsNonRetryable_OnBadRequest()
        {
            var handler = new StubHttpMessageHandler(HttpStatusCode.BadRequest);

            var act = async () => await CreateSut(handler)
                .GetByFileNameIfExists("file.csv", TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NonRetryableException>();
        }

    }


    public class CreateTests : FileImportStatusApiServiceTests
    {
        [Fact]
        public async Task Create_ReturnsId_WhenResponseIsSuccessful()
        {
            var dto = new FileImportDto { Id = 42, FileName = "file.csv", TotalRowsToProcess = 10 };
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(dto)
            });

            var result = await CreateSut(handler).Create("file.csv", 10, TestContext.Current.CancellationToken);

            result.Should().Be(42);
        }

        [Fact]
        public async Task Create_SendsExpectedPayload_ToExpectedUrl()
        {
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(new FileImportDto { Id = 1 })
            });

            await CreateSut(handler).Create("file.csv", 123, TestContext.Current.CancellationToken);

            handler.Requests.Should().ContainSingle();
            var request = handler.Requests[0];
            request.RequestUri!.PathAndQuery.Should().Be("/api/v1/systemadmin/fileimports");
            request.Method.Should().Be(HttpMethod.Post);
        }

        [Theory]
        [InlineData(HttpStatusCode.InternalServerError)]
        [InlineData(HttpStatusCode.RequestTimeout)]
        public async Task Create_ThrowsRetryable_OnTransientFailure(HttpStatusCode statusCode)
        {
            var handler = new StubHttpMessageHandler(statusCode);

            var act = async () => await CreateSut(handler)
                .Create("file.csv", 10, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<RetryableException>();
        }

        [Theory]
        [InlineData(HttpStatusCode.BadRequest)]
        [InlineData(HttpStatusCode.Forbidden)]
        public async Task Create_ThrowsNonRetryable_OnPermanentFailure(HttpStatusCode statusCode)
        {
            var handler = new StubHttpMessageHandler(statusCode);

            var act = async () => await CreateSut(handler)
                .Create("file.csv", 10, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NonRetryableException>();
        }

        [Fact]
        public async Task Create_ThrowsConflictException_OnConflictResponse()
        {
            var handler = new StubHttpMessageHandler(HttpStatusCode.Conflict);

            var act = async () => await CreateSut(handler)
                .Create("file.csv", 10, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<ConflictException>();
        }

        [Fact]
        public async Task Create_ThrowsNonRetryable_WhenResponseBodyIsNull()
        {
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("null", System.Text.Encoding.UTF8, "application/json")
            });

            var act = async () => await CreateSut(handler)
                .Create("file.csv", 10, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NonRetryableException>().WithMessage("*file.csv*");
        }

        [Fact]
        public async Task Create_ThrowsNonRetryable_WhenResponseBodyIsMalformedJson()
        {
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{ not-valid-json", System.Text.Encoding.UTF8, "application/json")
            });

            var act = async () => await CreateSut(handler)
                .Create("file.csv", 10, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NonRetryableException>().WithMessage("*file.csv*");
        }
    }

    public class TransportFailureTests : FileImportStatusApiServiceTests
    {
        [Fact]
        public async Task Request_ThrowsRetryable_WhenHttpRequestExceptionIsThrown()
        {
            var handler = new StubHttpMessageHandler(new HttpRequestException("connection refused"));

            var act = async () => await CreateSut(handler)
                .GetByFileName("file.csv", TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<RetryableException>();
        }

        [Fact]
        public async Task Request_ThrowsRetryable_WhenRequestTimesOut()
        {
            // A TaskCanceledException not tied to the caller's token represents an HttpClient timeout.
            var handler = new StubHttpMessageHandler(new TaskCanceledException("timeout"));

            var act = async () => await CreateSut(handler)
                .GetByFileName("file.csv", TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<RetryableException>();
        }

        [Fact]
        public async Task Request_ThrowsConflictException_OnConflictResponse()
        {
            var handler = new StubHttpMessageHandler(HttpStatusCode.Conflict);

            var act = async () => await CreateSut(handler)
                .GetByFileName("file.csv", TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<ConflictException>();
        }
    }

    public class MarkStatusTests : FileImportStatusApiServiceTests
    {
        [Theory]
        [InlineData(FileImportStatus.Transferred, "transferred")]
        [InlineData(FileImportStatus.Completed, "completed")]
        [InlineData(FileImportStatus.Failed, "failed")]
        public async Task MarkStatus_PostsToExpectedUrl(FileImportStatus status, string segment)
        {
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK));

            await CreateSut(handler).MarkStatus(42, status, TestContext.Current.CancellationToken);

            handler.Requests.Should().ContainSingle();
            var request = handler.Requests[0];
            request.Method.Should().Be(HttpMethod.Post);
            request.RequestUri!.PathAndQuery
                .Should().Be($"/api/v1/systemadmin/fileimports/42/{segment}");
        }

        [Theory]
        [InlineData(HttpStatusCode.InternalServerError)]
        [InlineData(HttpStatusCode.RequestTimeout)]
        public async Task MarkStatus_ThrowsRetryable_OnTransientFailure(HttpStatusCode statusCode)
        {
            var handler = new StubHttpMessageHandler(statusCode);

            var act = async () => await CreateSut(handler)
                .MarkStatus(1, FileImportStatus.Completed, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<RetryableException>();
        }

        [Theory]
        [InlineData(HttpStatusCode.BadRequest)]
        [InlineData(HttpStatusCode.NotFound)]
        public async Task MarkStatus_ThrowsNonRetryable_OnPermanentFailure(HttpStatusCode statusCode)
        {
            var handler = new StubHttpMessageHandler(statusCode);

            var act = async () => await CreateSut(handler)
                .MarkStatus(1, FileImportStatus.Completed, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NonRetryableException>();
        }

        [Theory]
        [InlineData(FileImportStatus.None)]
        [InlineData(FileImportStatus.Pending)]
        public async Task MarkStatus_ThrowsDomainException_WhenStatusHasNoUrlMapping(FileImportStatus status)
        {
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK));

            var act = async () => await CreateSut(handler)
                .MarkStatus(1, status, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<DomainException>();
            handler.Requests.Should().BeEmpty();
        }
    }
    public class MarkFailedTests : FileImportStatusApiServiceTests
    {
        [Fact]
        public async Task MarkFailed_PostsToExpectedUrl()
        {
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK));

            await CreateSut(handler).MarkFailed(7, "import failed", TestContext.Current.CancellationToken);

            handler.Requests.Should().ContainSingle();
            var request = handler.Requests[0];
            request.Method.Should().Be(HttpMethod.Post);
            request.RequestUri!.PathAndQuery
                .Should().Be("/api/v1/systemadmin/fileimports/7/failed");
        }

        [Fact]
        public async Task MarkFailed_SendsReasonAsJsonStringLiteralInBody()
        {
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.NoContent));

            await CreateSut(handler).MarkFailed(7, "file timed out", TestContext.Current.CancellationToken);

            var body = await handler.Requests[0].Content!.ReadAsStringAsync(TestContext.Current.CancellationToken);

            body.Should().Be("\"file timed out\"");
        }

        [Fact]
        public async Task MarkFailed_ThrowsRetryable_OnTransientFailure()
        {
            var handler = new StubHttpMessageHandler(HttpStatusCode.ServiceUnavailable);

            var act = async () => await CreateSut(handler)
                .MarkFailed(7, "import failed", TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<RetryableException>();
        }

        [Fact]
        public async Task MarkFailed_ThrowsNonRetryable_OnPermanentFailure()
        {
            var handler = new StubHttpMessageHandler(HttpStatusCode.BadRequest);

            var act = async () => await CreateSut(handler)
                .MarkFailed(7, "import failed", TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NonRetryableException>();
        }
    }

    public class MarkResetTests : FileImportStatusApiServiceTests
    {
        [Fact]
        public async Task MarkReset_PostsToExpectedUrl()
        {
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK));

            await CreateSut(handler).MarkReset(7, TestContext.Current.CancellationToken);

            handler.Requests.Should().ContainSingle();
            var request = handler.Requests[0];
            request.Method.Should().Be(HttpMethod.Post);
            request.RequestUri!.PathAndQuery
                .Should().Be("/api/v1/systemadmin/fileimports/7/reset");
        }

        [Fact]
        public async Task MarkReset_ThrowsRetryable_OnTransientFailure()
        {
            var handler = new StubHttpMessageHandler(HttpStatusCode.ServiceUnavailable);

            var act = async () => await CreateSut(handler)
                .MarkReset(7, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<RetryableException>();
        }

        [Fact]
        public async Task MarkReset_ThrowsNonRetryable_OnPermanentFailure()
        {
            var handler = new StubHttpMessageHandler(HttpStatusCode.BadRequest);

            var act = async () => await CreateSut(handler)
                .MarkReset(7, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<NonRetryableException>();
        }
    }
}