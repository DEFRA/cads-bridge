using System.Diagnostics;

namespace CadsBridge.Testing.Support.TestFixtures.Containers;

public static class DockerNetworkHelper
{
    public static void EnsureNetworkExists(string networkName)
    {
        var inspect = RunDocker($"network inspect {networkName}");

        if (inspect.ExitCode != 0)
        {
            var (ExitCode, _, StandardError) = RunDocker($"network create {networkName}");

            if (ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Failed to create Docker network '{networkName}'. Error: {StandardError}");
            }
        }
    }

    public static async ValueTask DeleteNetwork(string networkName)
    {
        var inspect = RunDocker($"network inspect {networkName}");

        if (inspect.ExitCode != 0)
            return; // network already gone

        var (ExitCode, _, _) = RunDocker($"network rm {networkName}");

        if (ExitCode != 0)
        {
            // swallow errors — containers may already be removed
            // or Docker Desktop may be shutting down
        }

        await ValueTask.CompletedTask;
    }

    private static (int ExitCode, string StandardOutput, string StandardError) RunDocker(string args)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = args,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        process.Start();
        process.WaitForExit();

        return (
            process.ExitCode,
            process.StandardOutput.ReadToEnd(),
            process.StandardError.ReadToEnd()
        );
    }
}