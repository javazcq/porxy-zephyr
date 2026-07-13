using System.Text.Json;

public interface IEndpointResolver
{
    // 从消息内容解析要转发到哪个 endpoint（返回完整 URL）
    string Resolve(JsonElement messageRoot);
}
