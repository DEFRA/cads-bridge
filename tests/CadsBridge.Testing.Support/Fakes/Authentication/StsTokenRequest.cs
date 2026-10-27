namespace CadsBridge.Testing.Support.Fakes.Authentication;

public sealed record StsTokenRequest
{
    public required string Sub { get; init; }
    public required string Aud { get; init; }
    public required string OrgId { get; init; }
    public required string AwsAccount { get; init; }
    public required string ServiceName { get; init; }
    public required string Environment { get; init; }
    public bool Expired { get; init; }
}