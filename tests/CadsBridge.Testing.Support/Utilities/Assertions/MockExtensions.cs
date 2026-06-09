using System.Linq.Expressions;
using Moq;

namespace CadsBridge.Testing.Support.Utilities.Assertions;

public static class MockExtensions
{
    extension<T>(Mock<T> mock) where T : class
    {
        public async Task AsyncVerify(
            Expression<Action<T>> verify,
            Func<Times> times,
            int backOffMilliSeconds = 400,
            int attempts = 5)
        {
            await mock.AsyncVerify(verify, times(), backOffMilliSeconds, attempts);
        }

        public async Task AsyncVerify(
            Expression<Action<T>> verify,
            Times times,
            int backOffMilliSeconds = 400,
            int attempts = 5)
        {
            await AsyncAssert.WaitForAssertion(() => { mock.Verify(verify, times); }, backOffMilliSeconds, attempts);
        }

        public async Task AsyncVerify<TResult>(
            Expression<Func<T, TResult>> verify,
            Func<Times> times,
            int backOffMilliSeconds = 400,
            int attempts = 5)
        {
            await mock.AsyncVerify(verify, times(), backOffMilliSeconds, attempts);
        }

        public async Task AsyncVerify<TResult>(
            Expression<Func<T, TResult>> verify,
            Times times,
            int backOffMilliSeconds = 400,
            int attempts = 5)
        {
            await AsyncAssert.WaitForAssertion(() => { mock.Verify(verify, times); }, backOffMilliSeconds, attempts);
        }
    }
}