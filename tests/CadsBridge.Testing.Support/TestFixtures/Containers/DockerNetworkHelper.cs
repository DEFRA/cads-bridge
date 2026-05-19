using System.Diagnostics;

namespace CadsBridge.Testing.Support.TestFixtures.Containers;

public static class DockerNetworkHelper
{
    public static void EnsureNetworkExists(string networkName)
    {
        var inspectProcess = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = $"network inspect {networkName}",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        inspectProcess.Start();
        inspectProcess.WaitForExit();

        if (inspectProcess.ExitCode != 0)
        {
            var createProcess = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "docker",
                    Arguments = $"network create {networkName}",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                }
            };
            createProcess.Start();
            createProcess.WaitForExit();

            if (createProcess.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Failed to create Docker network '{networkName}'. Error: {createProcess.StandardError.ReadToEnd()}");
            }
        }
    }
}