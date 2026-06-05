namespace CadsBridge.Testing.Support.Utilities.Assertions;

public static class AsyncAssert
{
    public static async Task WaitForAssertion(Func<Task> assertion, int backOffMilliSeconds = 400, int attempts = 5)
    {
        for (int attempt = 0; attempt < attempts; attempt++)
        {
            try
            {
                await assertion();
                return;
            }
            catch (Exception) when (attempt < attempts - 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(backOffMilliSeconds));
            }
        }
    }

    public static async Task WaitForAssertion(Action assertion, int backOffMilliSeconds = 400, int attempts = 5)
    {
        for (int attempt = 0; attempt < attempts; attempt++)
        {
            try
            {
                assertion();
                return;
            }
            catch (Exception) when (attempt < attempts - 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(backOffMilliSeconds));
            }
        }
    }
}