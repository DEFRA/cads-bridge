using System.Net;
using System.Net.Http.Json;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.ApiClients.Services;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.ApiClients.Services;

public class FileImportStatusApiServiceTests
{
    private readonly Mock<IHttpClientFactory> _httpClientFactory = new();

    private FileImportStatusApiService CreateSut(HttpMessageHandler handler)
    {
        var client = new HttpClient(handler) { BaseAddress = new Uri("http://test-api") };
        _httpClientFactory.Setup(x => x.CreateClient(nameof(ApiClientNames.CdsApi))).Returns(client);
        return new FileImportStatusApiService(_httpClientFactory.Object);
    }

    public class GetByFileNameTests : FileImportStatusApiServiceTests
    {
        [Fact]
        public async Task GetByFileName_ReturnsDto_WhenResponseIsSuccessful()
        {
            var dto = new FileImportStatusDto { Id = 1, FileName = "file.csv", TotalRowsToProcess = 100 };
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
                Content = JsonContent.Create(new FileImportStatusDto())
            });

            await CreateSut(handler).GetByFileName("my file.csv", TestContext.Current.CancellationToken);

            handler.Requests.Should().ContainSingle();
            handler.Requests[0].RequestUri!.PathAndQuery
                .Should().Be("/api/v1/systemadmin/fileimports/by-file-name?fileName=my%20file.csv");
        }

        [Theory]
        [InlineData(HttpStatusCode.NotFound)]
        [InlineData(HttpStatusCode.InternalServerError)]
        public async Task GetByFileName_Throws_WhenResponseIsNotSuccessful(HttpStatusCode statusCode)
        {
            var handler = new StubHttpMessageHandler(statusCode);

            var act = async () => await CreateSut(handler)
                .GetByFileName("file.csv", TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<HttpRequestException>();
        }
    }

    public class CreateTests : FileImportStatusApiServiceTests
    {
        [Fact]
        public async Task Create_ReturnsId_WhenResponseIsSuccessful()
        {
            var dto = new FileImportStatusDto { Id = 42, FileName = "file.csv", TotalRowsToProcess = 10 };
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
                Content = JsonContent.Create(new FileImportStatusDto { Id = 1 })
            });

            await CreateSut(handler).Create("file.csv", 123, TestContext.Current.CancellationToken);

            handler.Requests.Should().ContainSingle();
            var request = handler.Requests[0];
            request.RequestUri!.PathAndQuery.Should().Be("/api/v1/systemadmin/fileimports");
            request.Method.Should().Be(HttpMethod.Post);
        }

        [Theory]
        [InlineData(HttpStatusCode.BadRequest)]
        [InlineData(HttpStatusCode.InternalServerError)]
        public async Task Create_Throws_HttpRequestException_WhenResponseIsNotSuccessful(HttpStatusCode statusCode)
        {
            var handler = new StubHttpMessageHandler(statusCode);

            var act = async () => await CreateSut(handler)
                .Create("file.csv", 10, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<HttpRequestException>();
        }

        [Fact]
        public async Task Create_Throws_DomainException_WhenResponseBodyIsNull()
        {
            var handler = new StubHttpMessageHandler((_, _) => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("null", System.Text.Encoding.UTF8, "application/json")
            });

            var act = async () => await CreateSut(handler)
                .Create("file.csv", 10, TestContext.Current.CancellationToken);

            await act.Should().ThrowAsync<DomainException>().WithMessage("*file.csv*");
        }

        [Fact]
        public async Task Create_PropagatesConflict_AsHttpRequestExceptionWithStatusCode()
        {
            var handler = new StubHttpMessageHandler(HttpStatusCode.Conflict);

            var act = async () => await CreateSut(handler)
                .Create("file.csv", 10, TestContext.Current.CancellationToken);

            (await act.Should().ThrowAsync<HttpRequestException>())
                .Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
        }
    }
}

