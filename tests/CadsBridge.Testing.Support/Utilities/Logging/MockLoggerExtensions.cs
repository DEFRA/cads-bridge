using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Testing.Support.Utilities.Logging;

public static class MockLoggerExtensions
{
    public static Mock<ILogger<T>> EnableAllLogLevels<T>(this Mock<ILogger<T>> logger)
    {
        logger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        return logger;
    }
}