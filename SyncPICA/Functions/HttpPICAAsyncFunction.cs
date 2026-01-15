using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace HttpPicaAsync.Functions;

public class HttpPICAAsyncFunction
{
    private static readonly ConcurrentDictionary<string, JobState> Jobs = new();
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly ILogger _logger;

    public HttpPICAAsyncFunction(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<HttpPICAAsyncFunction>();
    }

    [Function("HttpPICAAsync")]
    public async Task<HttpResponseData> RunAsync(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "HttpPICAAsync")] HttpRequestData req)
    {
        var payload = await ParseRequestAsync(req);
        var jobId = Guid.NewGuid().ToString("N");
        var jobState = new JobState(jobId);

        if (!Jobs.TryAdd(jobId, jobState))
        {
            var conflict = req.CreateResponse(HttpStatusCode.Conflict);
            await conflict.WriteAsJsonAsync(new { message = "No se pudo crear el trabajo solicitado." });
            return conflict;
        }

        StartBackgroundJob(jobId, jobState, payload);

        var response = req.CreateResponse(HttpStatusCode.Accepted);
        await response.WriteAsJsonAsync(new
        {
            message = "Trabajo asincrono HttpPICAAsync iniciado.",
            jobId,
            statusEndpoint = $"{req.Url.GetLeftPart(UriPartial.Authority)}/api/HttpPICAAsync/{jobId}",
            cancelEndpoint = $"{req.Url.GetLeftPart(UriPartial.Authority)}/api/HttpPICAAsync/{jobId}/cancel"
        });

        return response;
    }

    [Function("HttpPICAAsyncStatus")]
    public async Task<HttpResponseData> GetStatusAsync(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "HttpPICAAsync/{jobId}")] HttpRequestData req,
        string jobId)
    {
        if (!Jobs.TryGetValue(jobId, out var state))
        {
            var notFound = req.CreateResponse(HttpStatusCode.NotFound);
            await notFound.WriteAsJsonAsync(new { message = $"No se encontro el trabajo {jobId}." });
            return notFound;
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            jobId,
            status = state.Status,
            createdAt = state.CreatedAt,
            completedAt = state.CompletedAt,
            messages = state.SnapshotMessages()
        });

        return response;
    }

    [Function("HttpPICAAsyncCancel")]
    public async Task<HttpResponseData> CancelAsync(
        [HttpTrigger(AuthorizationLevel.Function, "post", "delete", Route = "HttpPICAAsync/{jobId}/cancel")] HttpRequestData req,
        string jobId)
    {
        if (!Jobs.TryGetValue(jobId, out var state))
        {
            var notFound = req.CreateResponse(HttpStatusCode.NotFound);
            await notFound.WriteAsJsonAsync(new { message = $"No se encontro el trabajo {jobId}." });
            return notFound;
        }

        if (state.IsCompleted)
        {
            var alreadyCompleted = req.CreateResponse(HttpStatusCode.OK);
            await alreadyCompleted.WriteAsJsonAsync(new
            {
                jobId,
                message = "El trabajo ya se encuentra finalizado.",
                status = state.Status
            });
            return alreadyCompleted;
        }

        var cancelled = state.RequestCancellation();

        var response = req.CreateResponse(cancelled ? HttpStatusCode.Accepted : HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            jobId,
            message = cancelled ? "Se solicito la cancelacion del trabajo." : "La cancelacion ya estaba en curso.",
            status = state.Status
        });

        return response;
    }

    private void StartBackgroundJob(string jobId, JobState state, SyncPicaRequest request)
    {
        _ = Task.Run(async () =>
        {
            var token = state.Cancellation.Token;
            state.MarkRunning();

            try
            {
                for (var iteration = 1; iteration <= 10; iteration++)
                {
                    token.ThrowIfCancellationRequested();

                    var message = $"[{DateTimeOffset.UtcNow:O}] Mensaje {iteration} de 10 para {request.Name ?? "default"}.";
                    state.AddMessage(message);
                    _logger.LogInformation("{JobId} - {Message}", jobId, message);

                    if (iteration < 10)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(15), token);
                    }
                }

                var completionMessage = "Trabajo completado correctamente.";
                state.AddMessage(completionMessage);
                state.MarkCompleted("completed");
                _logger.LogInformation("{JobId} - {Message}", jobId, completionMessage);
            }
            catch (OperationCanceledException)
            {
                var cancelMessage = "Trabajo cancelado por solicitud del usuario.";
                state.AddMessage(cancelMessage);
                state.MarkCompleted("cancelled");
                _logger.LogWarning("{JobId} - {Message}", jobId, cancelMessage);
            }
            catch (Exception ex)
            {
                var errorMessage = $"Trabajo finalizado con errores: {ex.Message}";
                state.AddMessage(errorMessage);
                state.MarkCompleted("faulted");
                _logger.LogError(ex, "{JobId} - {Message}", jobId, errorMessage);
            }
        });
    }

    private static async Task<SyncPicaRequest> ParseRequestAsync(HttpRequestData req)
    {
        if (req.Body == null)
        {
            return new SyncPicaRequest();
        }

        using var reader = new StreamReader(req.Body);
        var body = await reader.ReadToEndAsync();

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

    private sealed class JobState
    {
        private readonly object _sync = new();
        private readonly List<string> _messages = new();
        private string _status = "pending";

        public JobState(string jobId)
        {
            JobId = jobId;
            Cancellation = new CancellationTokenSource();
            CreatedAt = DateTimeOffset.UtcNow;
        }

        public string JobId { get; }
        public CancellationTokenSource Cancellation { get; }
        public DateTimeOffset CreatedAt { get; }
        public DateTimeOffset? CompletedAt { get; private set; }

        public string Status
        {
            get
            {
                lock (_sync)
                {
                    return _status;
                }
            }
        }

        public bool IsCompleted
        {
            get
            {
                lock (_sync)
                {
                    return _status is "completed" or "cancelled" or "faulted";
                }
            }
        }

        public void MarkRunning()
        {
            UpdateStatus("running");
        }

        public void MarkCompleted(string status)
        {
            lock (_sync)
            {
                _status = status;
                CompletedAt = DateTimeOffset.UtcNow;
            }
        }

        public void AddMessage(string message)
        {
            lock (_sync)
            {
                _messages.Add(message);
            }
        }

        public IReadOnlyList<string> SnapshotMessages()
        {
            lock (_sync)
            {
                return _messages.ToArray();
            }
        }

        public bool RequestCancellation()
        {
            if (Cancellation.IsCancellationRequested)
            {
                return false;
            }

            UpdateStatus("cancelling");
            Cancellation.Cancel();
            return true;
        }

        private void UpdateStatus(string status)
        {
            lock (_sync)
            {
                _status = status;
            }
        }
    }
}

public sealed class SyncPicaRequest
{
    public string? Name { get; set; }
    public Dictionary<string, JsonElement>? Details { get; set; }
}
