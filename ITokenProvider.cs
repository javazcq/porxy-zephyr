using System.Threading;
using System.Threading.Tasks;

public interface ITokenProvider
{
    /// <summary>
    /// Returns a valid access token (cached). Token should not include "Bearer " prefix.
    /// </summary>
    Task<string?> GetTokenAsync(CancellationToken ct);
}
