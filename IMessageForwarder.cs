public interface IMessageForwarder
{
    /// <summary>
    /// Forward the consumed Kafka message to the resolved HTTP endpoint.
    /// Returns true when forwarded successfully (HTTP 2xx), false on final failure or when resolver returns empty.
    /// </summary>
    System.Threading.Tasks.Task<bool> ForwardAsync(Confluent.Kafka.ConsumeResult<string, string> consumeResult, System.Threading.CancellationToken ct);
}
