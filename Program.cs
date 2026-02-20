using System.Net;
using System.Net.Security;
using Agent.Services;
using Agent.Services.Agneta;
using Agent.Utils.Misc;
// using Agent.Services.Etcd; // REMOVED: No longer using etcd, using Kubernetes service discovery instead
// using Agent.Services.Grpc;
using Agent.Interfaces.Agneta;
// using dotnet_etcd; // REMOVED: No longer using etcd
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Logging;
using Agent.Services.PushOver;
using Agent.Modules.Pushover;
using Agent.Modules.Agneta;
using Agent.Utils.Globals;
using Agent.Models;
using Agent.Services.Storage;
using Agent.Services.Clms;
using Agent.Modules;
using Npgsql;
using Agent.Modules.Peer;
using Agent.Services.Cache;
using Agent.Utils;
using Agent.Interfaces.Infs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

// IMPORTANT
AgnetaHandler.disabled = true;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddLogging( logging =>
{
    logging.AddConsole();
    logging.SetMinimumLevel(LogLevel.Debug);
    // Suppress expected cancellation errors from gRPC (common during replication timeouts)
    logging.AddFilter("Grpc.AspNetCore.Server.ServerCallHandler", LogLevel.Warning);
});


builder.Services.AddGrpc(options => {
    options.MaxReceiveMessageSize = 1000 * 1024 * 1024;
    options.MaxSendMessageSize = 1000 * 1024 * 1024;
});

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing =>
    {
        tracing
            .SetResourceBuilder(Observability.CreateResourceBuilder())
            .AddSource("CrossV9.Agent")
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddOtlpExporter(otlp =>
            {
                otlp.Endpoint = new Uri(Observability.GetOtlpEndpoint());
                otlp.Protocol = OpenTelemetry.Exporter.OtlpExportProtocol.Grpc;
            });
    })
    .WithMetrics(metrics =>
    {
        metrics
            .SetResourceBuilder(Observability.CreateResourceBuilder())
            .AddMeter("CrossV9.Agent")
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddPrometheusExporter();
    });

ConfigureServices(builder.Services);

// Configure Kestrel to allow HTTP/2 without TLS
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(5000, o => o.Protocols = HttpProtocols.Http2);
    options.ListenAnyIP(5001, o => o.Protocols = HttpProtocols.Http1);
    //options.ListenLocalhost(5000, o => o.Protocols = HttpProtocols.Http2);
    //options.ListenLocalhost(5001, o => o.Protocols = HttpProtocols.Http1AndHttp2);
    options.Limits.MaxRequestBodySize = 1024 * 1024 * 1024;

    options.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(10);
    options.Limits.Http2.KeepAlivePingDelay = TimeSpan.FromSeconds(30);
    options.Limits.Http2.KeepAlivePingTimeout = TimeSpan.FromSeconds(60);
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Background services
builder.Services.AddHostedService<AgentLifeCycleService>();
builder.Services.AddHostedService<AgentRuntimeService>();

var app = builder.Build();

var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
lifetime.ApplicationStarted.Register(() => 
{
    Task.Run(async () =>
    {
        Globals.bootstraped = true;
        Console.WriteLine("$$$ BOOTSTRAPED $$$");
        
        // LOCAL MODE: Skip DHT initialization, set up standalone node
        // Also skip if bootstrap_node is null (standalone mode)
        bool isLocalMode = LocalModeDetector.IsLocalMode();
        Console.WriteLine($"[Agent] ApplicationStarted: Local mode={isLocalMode}, bootstrap_node={Globals.bootstrap_node ?? "null"}");
        
        if (isLocalMode || Globals.bootstrap_node == null)
        {
            Console.WriteLine("[Agent] Local/standalone mode - skipping DHT initialization");
            Globals.bootstrap_node = null;
            Globals._NODE.successor = new Agent.Models.M_Node() { id = Globals._NODE.id, ip = Globals._NODE.ip };
            Globals._NODE.predecessor = new Agent.Models.M_Node() { id = Globals._NODE.id, ip = Globals._NODE.ip };
            Console.WriteLine("[Agent] ✅ Standalone node configured - ready to accept requests");
        }
        else
        {
            Console.WriteLine($"[Agent] Distributed mode - joining network with bootstrap_node={Globals.bootstrap_node}");
            _ = await NodeService.JoinNetwork(Globals._NODE, Globals.bootstrap_node);
        }

        // ── WARMUP: Load ALL buckets/vectors from RocksDB into RAM ──
        // After this, search is pure in-memory — zero disk reads in hot path.
        try
        {
            var storageSvc = app.Services.GetRequiredService<INetworkFileStorageService>();
            if (storageSvc is Agent.Services.Storage.RocksDbStorageService rocksDbSvc)
            {
                rocksDbSvc.WarmUpBuckets();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Agent] WarmUpBuckets failed: {ex.Message}");
        }
    });
});

var clmsClientService = app.Services.GetRequiredService<ClmsClientService>();
ClmsHandler.SetClmsInstance(clmsClientService);

var pushoverClientService = app.Services.GetRequiredService<PushoverClientService>();
PushoverHandler.SetInstance(pushoverClientService);

var agnetaClientService = app.Services.GetRequiredService<AgnetaClientService>();
if(!AgnetaHandler.disabled)
{
    await agnetaClientService.ConnectAsync();
    AgnetaHandler.SetInstance(agnetaClientService);
}

//var networkFileSystemService = app.Services.GetRequiredService<NetworkFileStorageService>();
//var networkFileSystemService = app.Services.GetRequiredService<GcsSqlStorageService>(); // Drop in replacement for nfs with gcp
var networkFileSystemService = app.Services.GetRequiredService<INetworkFileStorageService>();
NetworkFileStorageHandler.SetInstance(networkFileSystemService);

// Initialize chunk cache service with configurable memory limits
var maxCacheSizeBytes = long.Parse(Environment.GetEnvironmentVariable("CHUNK_CACHE_SIZE_MB") ?? "512") * 1024 * 1024; // Default 512MB
var cacheTtlMinutes = int.Parse(Environment.GetEnvironmentVariable("CHUNK_CACHE_TTL_MINUTES") ?? "30"); // Default 30 minutes
var chunkCacheService = new Agent.Services.Cache.ChunkCacheService(
    networkFileSystemService,
    maxCacheSizeBytes: maxCacheSizeBytes,
    cacheTtl: TimeSpan.FromMinutes(cacheTtlMinutes)
);
Agent.Modules.Storage.ChunkCacheHandler.SetInstance(chunkCacheService);
Console.WriteLine($"[Cache] Initialized chunk cache: maxSize={maxCacheSizeBytes / (1024 * 1024)}MB, TTL={cacheTtlMinutes}min");

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// TODO: Uncomment for production
// app.UseHttpsRedirection();

app.UseRouting();
app.MapPrometheusScrapingEndpoint("/metrics");

app.MapGrpcService<GreeterService>();
app.MapGrpcService<FindPeerResponsibleService>();
app.MapGrpcService<GetNodeInfoService>();
app.MapGrpcService<GetPredecessorService>();
app.MapGrpcService<GetSuccessorService>();
app.MapGrpcService<GetHealthService>();
app.MapGrpcService<UpdatePredecessorService>();
app.MapGrpcService<UpdateSuccessorService>();
app.MapGrpcService<UpdateFingerTableService>();
app.MapGrpcService<SearchVectorService>();
app.MapGrpcService<StoreVectorService>();
app.MapGrpcService<ChunkReferenceServiceImpl>();

app.MapGet("/", () =>{ return "Hello world"; });
app.MapGet("/health", () => "true");

app.MapGet("/finger_table", () =>
{
    // Build rows using LINQ for readability
     var rows = string.Join("", Globals._NODE.fingerTable.Select(
        item => $"<tr><td>{item.Key.ToString()}</td><td>{item.Value.id} : {item.Value.ip}</td></tr>"
     ));

    // Use a string literal for the HTML structure
    var html = $@"
        <!doctype html>
        <html>
        <head>
            <title>Finger Table</title>
            <style>
                table {{
                    border-collapse: collapse;
                    width: 50%;
                }}
                th, td {{
                    border: 1px solid black;
                    text-align: left;
                    padding: 8px;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
            </style>
        </head>
        <body>
            <h1>Finger Table</h1>
            <h3>{Misc.GetLocalIPAddress()}</h3>
            <table>
                <tr>
                    <th>Key</th>
                    <th>Value</th>
                </tr>
                {rows}
            </table>
        </body>
        </html>";

    return Results.Content(html, "text/html");
});

app.MapGet("/network", async () => 
{
    var rows = "";

    List<string> _successors = new List<string>();
    _successors.Add(Globals._NODE.ip);
    if(Globals._NODE.successor.ip != Globals._NODE.ip)
    {
        _successors.Add(Globals._NODE.successor.ip);

        GetSuccessorService gss = new GetSuccessorService();
        while(true)
        {
            if(Globals._NODE.successor.ip == Globals._NODE.ip) { break; }

            M_Node res = await gss.ClientGet(_successors[^1]);
            string _ip = res.ip;
            if(_ip == Globals._NODE.ip) { break; }
            if(_successors.Contains(_ip)) { break; }

            _successors.Add(_ip);
        }
    }

    foreach (string _ip in _successors)
    {
        rows += $@"<tr><td>{_ip}</td></tr>";
    }

    var html = $@"
    <!doctype html>
    <html>
    <head>
        <title>Network</title>
        <style>
            body{{ background-color: #141414; }}
            table {{
                border-collapse: collapse;
                width: 50%;
            }}
            th, td {{
                border: 1px solid black;
                text-align: left;
                padding: 8px;
                background-color: #f2f2f2;
            }}
        </style>
    </head>
    <body>
        <h1 style='color:white;'>Finger Table</h1>
        <h3 style='color:white;'>{Misc.GetLocalIPAddress()}</h3>
        <h3 style='color:white;'>{Globals._NODE.id}</h3>
        <table>
            <tr>
                <th>Network successors list</th>
            </tr>
            {rows}
        </table>
    </body>
    </html>";
    return Results.Content(html, "text/html");
});

AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
{
    Console.WriteLine($"[UNHANDLED EXCEPTION]: {eventArgs.ExceptionObject}");
};

TaskScheduler.UnobservedTaskException += (sender, e) =>
{
    Console.WriteLine($"[UNOBSERVED TASK EXCEPTION]: {e.Exception}");
    e.SetObserved();
};

PushoverHandler.PushNotification($"Agent:{Globals.ETCD_ID}: Running");

app.Run();

// await AgnetaHandler.Close();
void ConfigureServices(IServiceCollection services)
{
    services.AddSingleton<ClmsClientService>(new ClmsClientService());
    services.AddSingleton<AgnetaClientService>(new AgnetaClientService("wss://agneta-loadbalancer.default.svc.cluster.local:443/log/ws"));
    services.AddSingleton<PushoverClientService>(new PushoverClientService());

    var pgbuilder = new NpgsqlConnectionStringBuilder
    {
        Host = Environment.GetEnvironmentVariable("DB_HOST"),
        Port = Convert.ToInt32(Environment.GetEnvironmentVariable("DB_PORT")),
        Username = Environment.GetEnvironmentVariable("DB_USER"),
        Password = Environment.GetEnvironmentVariable("DB_PASSWORD"),
        Database = Environment.GetEnvironmentVariable("DB_DATABASE") ?? "compressiondb",
        SslMode = SslMode.Disable,
        Pooling = true,
        MinPoolSize = 1,
        MaxPoolSize = 20,
        Timeout = 15,
        CommandTimeout = 30
    };

    var storageBackend = (Environment.GetEnvironmentVariable("STORAGE_BACKEND") ?? "local").ToLowerInvariant();
    services.AddSingleton<INetworkFileStorageService>(_ =>
    {
        if (storageBackend == "s3")
        {
            var endpoint = Environment.GetEnvironmentVariable("S3_ENDPOINT") ?? throw new InvalidOperationException("S3_ENDPOINT is required when STORAGE_BACKEND=s3");
            var bucket = Environment.GetEnvironmentVariable("S3_BUCKET") ?? throw new InvalidOperationException("S3_BUCKET is required when STORAGE_BACKEND=s3");
            var accessKey = Environment.GetEnvironmentVariable("S3_ACCESS_KEY") ?? throw new InvalidOperationException("S3_ACCESS_KEY is required when STORAGE_BACKEND=s3");
            var secretKey = Environment.GetEnvironmentVariable("S3_SECRET_KEY") ?? throw new InvalidOperationException("S3_SECRET_KEY is required when STORAGE_BACKEND=s3");
            var forcePathStyle = (Environment.GetEnvironmentVariable("S3_FORCE_PATH_STYLE") ?? "true").Equals("true", StringComparison.OrdinalIgnoreCase);
            var useSsl = (Environment.GetEnvironmentVariable("S3_USE_SSL") ?? "false").Equals("true", StringComparison.OrdinalIgnoreCase);

            Console.WriteLine($"[Storage] Using S3 backend endpoint={endpoint}, bucket={bucket}");
            return new S3StorageService(bucket, endpoint, accessKey, secretKey, forcePathStyle, useSsl, pgbuilder.ConnectionString);
        }

        if (storageBackend == "gcs")
        {
            var gcsBucket = Environment.GetEnvironmentVariable("GCS_BUCKET") ?? "cross-global-chunks";
            Console.WriteLine($"[Storage] Using GCS backend bucket={gcsBucket}");
            return new GcsSqlStorageService(gcsBucket, pgbuilder.ConnectionString);
        }

        if (storageBackend == "rocksdb")
        {
            var rocksPath = Environment.GetEnvironmentVariable("ROCKSDB_PATH") ?? "/data/chunks/rocksdb";
            Console.WriteLine($"[Storage] Using RocksDB backend path={rocksPath}");
            return new RocksDbStorageService(rocksPath, pgbuilder.ConnectionString);
        }

        var storageDir = Environment.GetEnvironmentVariable("CHUNK_STORAGE_DIR") ?? "/tmp/crossv9_chunks";
        Console.WriteLine($"[Storage] Using local backend dir={storageDir}");
        return new LocalFileStorageService(storageDir, pgbuilder.ConnectionString);
    });
}
