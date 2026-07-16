using CadsBridge.Core.Correlation;
using FluentAssertions;
using Microsoft.AspNetCore.Http;

namespace CadsBridge.Tests.Component.Correlation;

public class CorrelationIdMiddlewareTests
{
    private const string HeaderName = "x-cdp-request-id";

    public CorrelationIdMiddlewareTests()
    {
        CorrelationIdContext.Value = null;
    }

    private static DefaultHttpContext CreateHttpContext() => new();

    [Fact]
    public async Task UsesExistingCorrelationId_WhenHeaderIsPresent()
    {
        // Arrange
        var context = CreateHttpContext();
        var existingId = Guid.NewGuid().ToString();
        context.Request.Headers[HeaderName] = existingId;

        string? capturedValue = null;
        Task next(HttpContext ctx)
        {
            capturedValue = CorrelationIdContext.Value;
            return Task.CompletedTask;
        }

        var middleware = new CorrelationIdMiddleware(next);

        // Act
        await middleware.Invoke(context);

        // Assert
        capturedValue.Should().Be(existingId);
        context.Response.Headers[HeaderName].FirstOrDefault().Should().Be(existingId);
    }

    [Fact]
    public async Task GeneratesCorrelationId_WhenHeaderIsMissing()
    {
        // Arrange
        var context = CreateHttpContext();

        string? capturedValue = null;

        Task next(HttpContext ctx)
        {
            capturedValue = CorrelationIdContext.Value;
            return Task.CompletedTask;
        }

        var middleware = new CorrelationIdMiddleware(next);

        // Act
        await middleware.Invoke(context);

        // Assert
        capturedValue.Should().NotBeNullOrWhiteSpace();
        context.Request.Headers[HeaderName].FirstOrDefault().Should().Be(capturedValue);
        context.Response.Headers[HeaderName].FirstOrDefault().Should().Be(capturedValue);
    }

    [Fact]
    public async Task DoesNotOverwriteExistingCorrelationId()
    {
        // Arrange
        var context = CreateHttpContext();
        var existingId = Guid.NewGuid().ToString();
        context.Request.Headers[HeaderName] = existingId;

        string? capturedValue = null;

        Task next(HttpContext ctx)
        {
            capturedValue = CorrelationIdContext.Value;
            return Task.CompletedTask;
        }

        var middleware = new CorrelationIdMiddleware(next);

        // Act
        await middleware.Invoke(context);

        // Assert
        capturedValue.Should().Be(existingId);
        context.Response.Headers[HeaderName].FirstOrDefault().Should().Be(existingId);
    }

    [Fact]
    public async Task CallsNextMiddleware()
    {
        // Arrange
        var context = CreateHttpContext();
        var nextCalled = false;

        Task next(HttpContext ctx)
        {
            nextCalled = true;
            return Task.CompletedTask;
        }

        var middleware = new CorrelationIdMiddleware(next);

        // Act
        await middleware.Invoke(context);

        // Assert
        nextCalled.Should().BeTrue();
    }

    [Fact]
    public async Task ResponseHeaderIsSetEvenIfNextThrows()
    {
        // Arrange
        var context = CreateHttpContext();

        static Task next(HttpContext ctx)
        {
            throw new Exception("Boom");
        }

        var middleware = new CorrelationIdMiddleware(next);

        // Act
        Func<Task> act = async () => await middleware.Invoke(context);

        // Assert
        await act.Should().ThrowAsync<Exception>();

        context.Response.Headers[HeaderName]
            .FirstOrDefault()
            .Should()
            .NotBeNullOrWhiteSpace();
    }
}