using System.Net;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace SyncPICA.Functions;

public class SyncPICAFunction
{
    private readonly ILogger _logger;
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public SyncPICAFunction(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<SyncPICAFunction>();
    }

    [Function("SyncPICA")]
    public async Task<HttpResponseData> RunAsync(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "SyncPICA")] HttpRequestData req)
    {
        _logger.LogInformation("SyncPICA recibió una solicitud.");

        var payload = await ParseRequestAsync(req);
        var result = await PerformAsyncTask(payload);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            message = "Tarea asíncrona ejecutada correctamente",
            result
        });

        return response;
    }

    private static async Task<SyncPicaRequest> ParseRequestAsync(HttpRequestData req)
    {
        if (req.Body == null)
        {
            return new SyncPicaRequest();
        }

        var body = await new StreamReader(req.Body).ReadToEndAsync();

        if (string.IsNullOrWhiteSpace(body))
        {
            return new SyncPicaRequest();
        }

        try
        {
            return JsonSerializer.Deserialize<SyncPicaRequest>(body, SerializerOptions) ?? new SyncPicaRequest();
        }
        catch (JsonException)
        {
            return new SyncPicaRequest();
        }
    }

    private static async Task<SyncPicaResult> PerformAsyncTask(SyncPicaRequest request)
    {
        await Task.Delay(TimeSpan.FromSeconds(1));

        return new SyncPicaResult
        {
            Status = "completed",
            Task = request.Name ?? "default",
            Details = request.Details ?? new Dictionary<string, JsonElement>()
        };
    }
}

public sealed class SyncPicaRequest
{
    public string? Name { get; set; }
    public Dictionary<string, JsonElement>? Details { get; set; }
}

public sealed class SyncPicaResult
{
    public string Status { get; set; } = string.Empty;
    public string Task { get; set; } = string.Empty;
    public Dictionary<string, JsonElement> Details { get; set; } = new();
}
