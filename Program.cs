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
    options.MaxReceiveMessageSize = 32 * 1024 * 1024;
    options.MaxSendMessageSize = 32 * 1024 * 1024;
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
    options.Limits.MaxRequestBodySize = null;

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

// ── AUTO-DETECT AVAILABLE MEMORY (top-level, shared by startup handler and chunk cache init) ──
// GC.GetGCMemoryInfo().TotalAvailableMemoryBytes respects cgroup limits (K8s pod limit)
// and falls back to physical RAM when no limit is set. This allows per-node sizing
// automatically — 16 GB nodes get smaller caches, 64 GB nodes get larger ones.
long totalAvailableMemory = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
Console.WriteLine($"[Agent] Detected available memory: {totalAvailableMemory / (1024 * 1024)}MB ({totalAvailableMemory / (1024L * 1024 * 1024)}GB)");

// ── BUCKET CACHE SIZE: auto-detect or use configured value ──
// NOTE: On nodes without K8s memory limits, totalAvailableMemory = FULL node RAM.
// The agent shares the node with gateway, cross, redis, kubelet, etc.
// We use conservative percentages (25% bucket + 5% chunk = 30% total) to leave
// plenty of room for other workloads and RocksDB native memory.
// The system memory guard in BucketCacheManager reads /proc/meminfo every 2s
// and ensures 10% of node RAM stays free NO MATTER WHAT.
var bucketCacheRaw = Environment.GetEnvironmentVariable("BUCKET_CACHE_MAX_GB") ?? "0";
long bucketCacheMaxBytes;
if (bucketCacheRaw == "0" || bucketCacheRaw.Equals("auto", StringComparison.OrdinalIgnoreCase))
{
    // 10% of RAM for app-level M_Bucket cache (write-active buckets only).
    // Cold bucket searches now go through SearchBucketDirect on RocksDB's native block cache
    // (25% of RAM), so the app-level cache only needs to hold recently-written buckets
    // for the store-side similarity scan in InsertData.
    bucketCacheMaxBytes = (long)(totalAvailableMemory * 0.10);
    Console.WriteLine($"[Agent] Bucket cache: AUTO-SIZED to {bucketCacheMaxBytes / (1024 * 1024)}MB (10% of {totalAvailableMemory / (1024 * 1024)}MB)");
}
else
{
    var bucketCacheMaxGb = long.Parse(bucketCacheRaw);
    bucketCacheMaxBytes = bucketCacheMaxGb * 1024L * 1024 * 1024;
    // Safety cap: never exceed 50% of available memory for bucket cache alone
    long maxSafe = (long)(totalAvailableMemory * 0.50);
    if (bucketCacheMaxBytes > maxSafe)
    {
        Console.WriteLine($"[Agent] ⚠️ Bucket cache {bucketCacheMaxBytes / (1024 * 1024)}MB exceeds 50% of available memory ({maxSafe / (1024 * 1024)}MB). Capping to {maxSafe / (1024 * 1024)}MB.");
        bucketCacheMaxBytes = maxSafe;
    }
}

var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();

// ── GRACEFUL SHUTDOWN: Flush all write batchers before exit ──
// On SIGTERM (Kubernetes pod termination), ensure all pending RocksDB writes
// are durably written to SSD before the process exits.
// Belt-and-suspenders alongside DI Dispose: explicit is safer than relying on
// the DI container ordering, especially during OOM-related kills.
lifetime.ApplicationStopping.Register(() =>
{
    Console.WriteLine("[Agent] ⚠️ ApplicationStopping — stopping background evictors and flushing write batchers...");
    if (Agent.Services.Cache.BucketCacheManager.L1Enabled)
    {
        try
        {
            Agent.Services.Cache.BucketCacheManager.Shutdown();
            Console.WriteLine("[Agent] ✅ BucketCacheManager stopped.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Agent] ❌ Error stopping BucketCacheManager: {ex.Message}");
        }
    }
    try
    {
        NetworkFileStorageHandler.FlushPendingWrites();
        Console.WriteLine("[Agent] ✅ Write batchers flushed successfully.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Agent] ❌ Error flushing write batchers: {ex.Message}");
    }
});

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

        // ── BUCKET CACHE: uses pre-computed bucketCacheMaxBytes from top-level ──
        var bucketEvictionSec = int.Parse(Environment.GetEnvironmentVariable("BUCKET_CACHE_EVICTION_SEC") ?? "10");
        var bucketHighWaterPct = double.Parse(Environment.GetEnvironmentVariable("BUCKET_CACHE_HIGH_WATER_PCT") ?? "0.70",
            System.Globalization.CultureInfo.InvariantCulture);
        var bucketLowWaterPct = double.Parse(Environment.GetEnvironmentVariable("BUCKET_CACHE_LOW_WATER_PCT") ?? "0.50",
            System.Globalization.CultureInfo.InvariantCulture);
        var bucketHardCeilingPct = double.Parse(Environment.GetEnvironmentVariable("BUCKET_CACHE_HARD_CEILING_PCT") ?? "0.90",
            System.Globalization.CultureInfo.InvariantCulture);

        // ── L1 CACHE TOGGLE ──
        Agent.Services.Cache.BucketCacheManager.L1Enabled =
            (Environment.GetEnvironmentVariable("L1_CACHE_ENABLED") ?? "false")
            .Equals("true", StringComparison.OrdinalIgnoreCase);
        Console.WriteLine($"[Agent] L1 bucket cache: {(Agent.Services.Cache.BucketCacheManager.L1Enabled ? "ENABLED" : "DISABLED (RocksDB-direct mode)")}");

        // ── WARMUP: Load buckets from RocksDB into RAM (up to L1 budget) ──
        // Hot buckets go to L1 (RAM). Cold buckets stay on L2 (RocksDB disk),
        // loaded on-demand during search/store with ~0.1-0.3ms latency.
        try
        {
            var storageSvc = app.Services.GetRequiredService<INetworkFileStorageService>();
            if (storageSvc is Agent.Services.Storage.RocksDbStorageService rocksDbSvc)
            {
                // Always register bucket storage reference so GetBucketStorage() works
                Agent.Services.Cache.BucketCacheManager.SetBucketStorage(rocksDbSvc.BucketStorage);

                if (Agent.Services.Cache.BucketCacheManager.L1Enabled)
                {
                    Agent.Services.Cache.BucketCacheManager.Initialize(
                        rocksDbSvc.BucketStorage, bucketCacheMaxBytes, bucketEvictionSec,
                        totalAvailableMemory: totalAvailableMemory,
                        highWaterPct: bucketHighWaterPct,
                        lowWaterPct: bucketLowWaterPct,
                        hardCeilingPct: bucketHardCeilingPct);

                    rocksDbSvc.WarmUpBuckets(bucketCacheMaxBytes);
                }

                // Initialize live stats counters (O(1) from persisted, or one-time scan)
                rocksDbSvc.InitializeStats();

                // Persist stats counters every 60 seconds (survives crashes)
                var statsTimer = new System.Threading.Timer(_ =>
                {
                    try { rocksDbSvc.PersistStatsCounters(); }
                    catch (Exception ex2) { Console.WriteLine($"[Agent] Stats persist failed: {ex2.Message}"); }
                }, null, TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(60));

                // Compact LOH every 5 minutes to prevent memory fragmentation on long runs.
                // Without this, the LOH accumulates holes from large temporary arrays (float[], byte[])
                // that can't be reused, causing RSS to grow while actual live data stays flat.
                var lohCompactTimer = new System.Threading.Timer(_ =>
                {
                    System.Runtime.GCSettings.LargeObjectHeapCompactionMode =
                        System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
                    GC.Collect(2, GCCollectionMode.Optimized, blocking: false);
                }, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Agent] WarmUpBuckets failed: {ex.Message}");
        }

        // ── READINESS GATE: Signal that this agent is ready to serve ──
        // Until this flag is true, the K8s readiness probe returns 503,
        // so the pod IP is NOT in the headless service DNS.
        // This prevents Cross/Gateway from routing to a cold agent.
        Globals.IsReady = true;
        Console.WriteLine("[Agent] ✅ Readiness gate OPEN — agent is ready to serve");
        Console.WriteLine("[Agent] ⚠️  REPLICATION IS DISABLED — single-copy storage only. " +
            "If this pod/disk is lost, chunks owned by this agent are NOT recoverable. " +
            "Use replicated PersistentVolumes or external backup to prevent data loss.");
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
// If CHUNK_CACHE_SIZE_MB=0 (or "auto"), auto-size to 5% of available memory.
// Otherwise use the configured value, but cap at 10% of available memory.
var chunkCacheRaw = Environment.GetEnvironmentVariable("CHUNK_CACHE_SIZE_MB") ?? "0";
long maxCacheSizeBytes;
if (chunkCacheRaw == "0" || chunkCacheRaw.Equals("auto", StringComparison.OrdinalIgnoreCase))
{
    maxCacheSizeBytes = (long)(totalAvailableMemory * 0.05);
    Console.WriteLine($"[Cache] Chunk cache: AUTO-SIZED to {maxCacheSizeBytes / (1024 * 1024)}MB (5% of {totalAvailableMemory / (1024 * 1024)}MB)");
}
else
{
    maxCacheSizeBytes = long.Parse(chunkCacheRaw) * 1024 * 1024;
    long maxSafe = (long)(totalAvailableMemory * 0.10);
    if (maxCacheSizeBytes > maxSafe)
    {
        Console.WriteLine($"[Cache] ⚠️ Chunk cache {maxCacheSizeBytes / (1024 * 1024)}MB exceeds 10% of available memory ({maxSafe / (1024 * 1024)}MB). Capping to {maxSafe / (1024 * 1024)}MB.");
        maxCacheSizeBytes = maxSafe;
    }
}
var cacheTtlMinutes = int.Parse(Environment.GetEnvironmentVariable("CHUNK_CACHE_TTL_MINUTES") ?? "30");
var chunkCacheService = new Agent.Services.Cache.ChunkCacheService(
    networkFileSystemService,
    maxCacheSizeBytes: maxCacheSizeBytes,
    cacheTtl: TimeSpan.FromMinutes(cacheTtlMinutes)
);
Agent.Modules.Storage.ChunkCacheHandler.SetInstance(chunkCacheService);

if (Agent.Services.Cache.BucketCacheManager.L1Enabled)
{
    Agent.Services.Cache.BucketCacheManager.RegisterChunkCacheEvictor(() =>
    {
        chunkCacheService.ForceEvict(0.0);
    });
}

// Log total memory budget
long totalCacheBudget = bucketCacheMaxBytes + maxCacheSizeBytes;
long headroom = totalAvailableMemory - totalCacheBudget;
Console.WriteLine($"[Cache] Initialized chunk cache: maxSize={maxCacheSizeBytes / (1024 * 1024)}MB, TTL={cacheTtlMinutes}min");
Console.WriteLine($"[Agent] Memory budget: buckets={bucketCacheMaxBytes / (1024 * 1024)}MB + chunks={maxCacheSizeBytes / (1024 * 1024)}MB = {totalCacheBudget / (1024 * 1024)}MB total, headroom={headroom / (1024 * 1024)}MB ({headroom * 100 / totalAvailableMemory}%)");

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
app.MapGrpcService<Agent.Services.Grpc.StorageStatsService>();
app.MapGrpcService<SearchLanesService>();

app.MapGet("/", () =>{ return "Hello world"; });
app.MapGet("/health", () => "true"); // Liveness: always alive once Kestrel is up
app.MapGet("/ready", () => Globals.IsReady
    ? Results.Ok("ready")
    : Results.StatusCode(503)); // Readiness: only after WarmUpBuckets completes

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
            // RocksDB block caches use NATIVE memory (no GC pressure).
            // Bucket block cache is the PRIMARY cache for search — holds SST blocks containing
            // vector records. 25% of RAM = ~8GB on 32GB nodes, enough for ~27M vectors.
            // Cold bucket searches go through SearchBucketDirect which reads from this cache
            // instead of materializing M_Bucket objects in the managed heap.
            long availMem = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
            long bucketBlockCacheMb = Agent.Services.Cache.BucketCacheManager.L1Enabled
                ? Math.Max(256, availMem * 25 / 100 / (1024 * 1024))
                : Math.Max(256, availMem * 40 / 100 / (1024 * 1024));
            long chunkBlockCacheMb = Math.Max(64, availMem * 5 / 100 / (1024 * 1024));
            Console.WriteLine($"[Storage] Using RocksDB backend path={rocksPath}, bucket block cache={bucketBlockCacheMb}MB, chunk block cache={chunkBlockCacheMb}MB");
            return new RocksDbStorageService(rocksPath, pgbuilder.ConnectionString,
                bucketBlockCacheMb: bucketBlockCacheMb, chunkBlockCacheMb: chunkBlockCacheMb);
        }

    var storageDir = Environment.GetEnvironmentVariable("CHUNK_STORAGE_DIR") ?? "/tmp/crossv9_chunks";
        Console.WriteLine($"[Storage] Using local backend dir={storageDir}");
        return new LocalFileStorageService(storageDir, pgbuilder.ConnectionString);
    });
}
