using System.Text.RegularExpressions;

namespace CadsBridge.Infrastructure.DataLoad.Services.Testing;

public class TestingConstants
{
    public static readonly Regex ValidE2EFilenamePattern = new(@"^CTSM_CADS_ETE_[a-zA-Z0-9\-_]+[^\/\\].(csv|CSV)$", RegexOptions.CultureInvariant);
}