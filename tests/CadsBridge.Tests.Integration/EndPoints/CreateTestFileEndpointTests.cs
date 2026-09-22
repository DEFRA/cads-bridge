using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Endpoints.Testing.Models;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using CadsBridge.Testing.Support.Utilities.Assertions;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;
using System.Net;
using System.Text.Json;
using CadsBridge.Infrastructure.Json;

namespace CadsBridge.Tests.Integration.EndPoints;

[Collection("CadsBridgeIntegration"), Trait("Dependence", "testcontainers")]
public class CreateTestFileEndpointTests
{
    private const string FileName = "CTSM_CADS_TEST_FULL_BATCH1_MYTABLE_2026-07-10-120000.csv";

    [Fact]
    public async Task CreateTestFile_WithValidRequest_UploadsFileToExternalS3Bucket()
    {
        await using var fixture = new ApiContainerWithEnvsFixture(new Dictionary<string, string>
        {
            ["EnableTestEndpoints"] = "true"
        });
        await fixture.InitializeAsync();

        var request = new CreateTestFileRequest(FileName, "some test file content", DataSourceType.CtsBulk);
        var content = HttpContentUtility.CreateApplicationJsonAsStringContent(request);

        // Act
        var response = await fixture.HttpClient!.PostAsync(
            "/api/v1/test-support/files", content, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var responseBody = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        var uploadResponse = JsonSerializer.Deserialize<CreateTestFileResponse>(responseBody, JsonDefaults.DefaultOptions);

        uploadResponse.Should().NotBeNull();
        uploadResponse!.FileName.Should().Be(FileName);
        uploadResponse.ExternalS3Bucket.Should().Be(TestS3Constants.TestCadsBridgeExternalBucketName);
        uploadResponse.ExternalS3Key.Should().Be(FileName);
        uploadResponse.SizeBytes.Should().BeGreaterThan(0);

        await AsyncAssert.WaitForAssertion(async () =>
        {
            var getObjectResponse = await fixture.LocalStackFixture.S3Client.GetObjectAsync(
                new GetObjectRequest
                {
                    BucketName = TestS3Constants.TestCadsBridgeExternalBucketName,
                    Key = FileName
                },
                TestContext.Current.CancellationToken);

            getObjectResponse.HttpStatusCode.Should().Be(HttpStatusCode.OK);
            getObjectResponse.ContentLength.Should().BeGreaterThan(0);
        });
    }
}