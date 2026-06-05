using System.Linq.Expressions;
using Moq;

namespace CadsBridge.Testing.Support.Utilities.Assertions;

public static class MockExtensions
{
    public static async Task AsyncVerify<T>(
        this Mock<T> mock,
        Expression<Action<T>> verify,
        Func<Times> times,
        int backOffMilliSeconds = 400,
        int attempts = 5) where T : class
    {
        await mock.AsyncVerify(verify, times(), backOffMilliSeconds, attempts);
    }

    public static async Task AsyncVerify<T>(
        this Mock<T> mock,
        Expression<Action<T>> verify,
        Times times,
        int backOffMilliSeconds = 400,
        int attempts = 5) where T : class
    {
        await AsyncAssert.WaitForAssertion(() => { mock.Verify(verify, times); }, backOffMilliSeconds, attempts);
    }
}