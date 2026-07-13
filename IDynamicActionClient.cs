public interface IDynamicActionClient
{
    /// <summary>
    /// Invoke a dynamic action by name with the provided JSON payload. Returns true on HTTP 2xx.
    /// </summary>
    System.Threading.Tasks.Task<bool> InvokeActionAsync(string actionName, string jsonPayload, System.Threading.CancellationToken ct);
}
