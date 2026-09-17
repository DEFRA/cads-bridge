namespace CadsBridge.Application.DataLoad.Sources;

/// <summary>
/// Encapsulates the source-specific rules that ScanTaskInfo attributes cannot express:
/// filename grammar/validation and decryption-key derivation.
/// </summary>
public interface IDataSourceStrategy
{
    /// <summary>
    /// True when the filename is well-formed for this source and matches the expected
    /// scan type (e.g. "BULK" or "DELTA").
    /// </summary>
    bool IsFileNameValidForType(string fileName, string expectedType);

    /// <summary>
    /// Derives the decryption password for a source file using the source's scheme.
    /// </summary>
    string DeriveDecryptionPassword(string fileName);
}