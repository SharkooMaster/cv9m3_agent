using System.Net;
using System.Net.Security;
using Agent.Services;
using Agent.Services.Agneta;
using Agent.Utils.Misc;
using Agent.Services.Etcd;
// using Agent.Services.Grpc;
using Agent.Interfaces.Agneta;
using dotnet_etcd;
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
    // options.Limits.Http2.KeepAlivePingDelay = TimeSpan.FromSeconds(30);
    // options.Limits.Http2.KeepAlivePingTimeout = TimeSpan.FromSeconds(60);
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Background services
builder.Services.AddHostedService<AgentLifeCycleService>();
builder.Services.AddHostedService<AgentRuntimeService>();

// Periodic Merkle anti-entropy. Disabled by default (opt-in via
// ANTI_ENTROPY_ENABLED=true). When on, every agent compares its
// chunk-store summary with each peer's every 5 minutes and surfaces
// divergence; with ANTI_ENTROPY_AUTO_REPAIR=true it pulls the
// missing chunks. See AntiEntropyService for the full algorithm.
builder.Services.AddHostedService<Agent.Services.Background.AntiEntropyService>();

var app = builder.Build();

// ── AUTO-DETECT AVAILABLE MEMORY (top-level, shared by startup handler and chunk cache init) ──
// GC.GetGCMemoryInfo().TotalAvailableMemoryBytes respects:
//   1. DOTNET_GCHeapHardLimit env var (managed heap cap)
//   2. cgroup memory limit (K8s pod limit)
//   3. physical RAM (fallback)
// IMPORTANT: This value constrains the MANAGED heap only. RocksDB block caches
// are native (C++ malloc) and sit OUTSIDE this budget. The sizing below must
// account for both managed and native memory within the container's total.
long totalAvailableMemory = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
Console.WriteLine($"[Agent] Detected available memory: {totalAvailableMemory / (1024 * 1024)}MB ({totalAvailableMemory / (1024L * 1024 * 1024)}GB)");

// ── BUCKET CACHE SIZE: auto-detect or use configured value ──
var bucketCacheRaw = Environment.GetEnvironmentVariable("BUCKET_CACHE_MAX_GB") ?? "0";
long bucketCacheMaxBytes;
if (bucketCacheRaw == "0" || bucketCacheRaw.Equals("auto", StringComparison.OrdinalIgnoreCase))
{
    bucketCacheMaxBytes = (long)(totalAvailableMemory * 0.10);
    Console.WriteLine($"[Agent] Bucket cache: AUTO-SIZED to {bucketCacheMaxBytes / (1024 * 1024)}MB (10% of {totalAvailableMemory / (1024 * 1024)}MB)");
}
else
{
    var bucketCacheMaxGb = long.Parse(bucketCacheRaw);
    bucketCacheMaxBytes = bucketCacheMaxGb * 1024L * 1024 * 1024;
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
    try
    {
        Agent.Services.Cache.BucketCacheManager.Shutdown();
        Console.WriteLine("[Agent] ✅ BucketCacheManager stopped.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Agent] ❌ Error stopping BucketCacheManager: {ex.Message}");
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

                // Always start the memory guard (monitors /proc/meminfo, triggers
                // emergency chunk cache eviction + GC when node memory is low).
                // When L1 is disabled, bucket watermark eviction is a no-op, but
                // the system memory guard still protects against OOM.
                Agent.Services.Cache.BucketCacheManager.Initialize(
                    rocksDbSvc.BucketStorage, bucketCacheMaxBytes, bucketEvictionSec,
                    totalAvailableMemory: totalAvailableMemory,
                    highWaterPct: bucketHighWaterPct,
                    lowWaterPct: bucketLowWaterPct,
                    hardCeilingPct: bucketHardCeilingPct);

                if (Agent.Services.Cache.BucketCacheManager.L1Enabled)
                {
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

                // LOH compaction: agent's BatchGet/BatchSearch hot path churns
                // hundreds of float[] vectors and byte[] chunks > 85 KB per request,
                // all of which land on the LOH and never compact unless explicitly
                // requested. Was every 5 min with `Optimized` mode — but `Optimized`
                // lets the GC skip the request if it doesn't think a Gen2 is warranted,
                // so peak frag drifted to 50 %+ between actually-honoured compactions
                // (visible in the controlcenter fleet panel). 60 s cadence with `Forced`
                // mode keeps peak frag under ~20 % at a cost of ~50–150 ms STW pause
                // per minute, which is well below the relaxed liveness probe budget.
                var lohCompactTimer = new System.Threading.Timer(_ =>
                {
                    System.Runtime.GCSettings.LargeObjectHeapCompactionMode =
                        System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
                    GC.Collect(2, GCCollectionMode.Forced, blocking: false);
                }, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Agent] Startup cache init failed: {ex.GetType().Name}: {ex.Message}");
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

// Always register chunk cache evictor so the system memory guard can
// clear chunk cache when node memory is critically low.
Agent.Services.Cache.BucketCacheManager.RegisterChunkCacheEvictor(() =>
{
    chunkCacheService.ForceEvict(0.0);
});

// Log total memory budget (including native RocksDB allocations)
long totalManagedBudget = bucketCacheMaxBytes + maxCacheSizeBytes;
long managedHeadroom = totalAvailableMemory - totalManagedBudget;
Console.WriteLine($"[Cache] Initialized chunk cache: maxSize={maxCacheSizeBytes / (1024 * 1024)}MB, TTL={cacheTtlMinutes}min");
Console.WriteLine($"[Agent] Memory budget: managed(buckets={bucketCacheMaxBytes / (1024 * 1024)}MB + chunks={maxCacheSizeBytes / (1024 * 1024)}MB = {totalManagedBudget / (1024 * 1024)}MB), managed headroom={managedHeadroom / (1024 * 1024)}MB ({managedHeadroom * 100 / totalAvailableMemory}%) [RocksDB native is additional]");

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// TODO: Uncomment for production
// app.UseHttpsRedirection();

app.UseRouting();
app.MapPrometheusScrapingEndpoint("/metrics");

// Tiny pod-self-reporting endpoint pulled by the control-center every ~30 s.
// Allocation-bounded; never touches the RocksDB hot path.
Agent.Utils.RuntimeStatsEndpoint.Map(app, "agent");

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
app.MapGrpcService<Agent.Services.Grpc.VnodeStreamingGrpcService>();

app.MapGet("/", () =>{ return "Hello world"; });
app.MapGet("/health", () => "true"); // Liveness: always alive once Kestrel is up
app.MapGet("/ready", () => Globals.IsReady
    ? Results.Ok("ready")
    : Results.StatusCode(503)); // Readiness: only after WarmUpBuckets completes

// ── Destructive admin endpoint (Phase 8 dev tooling) ─────────────────
// Triggered by cross's POST /admin/cluster/wipe. Kills the process so the
// StatefulSet/Deployment restarts the pod fresh; combined with
// `--set dev.fastReset=true` (emptyDir for /data/chunks) the restart
// effectively drops the agent's RocksDB.
//
// Gated on ADMIN_DESTRUCTIVE_ENABLED=true so a misrouted request in
// production can never wipe live data.
app.MapPost("/admin/wipe", () =>
{
    var gate = System.Environment.GetEnvironmentVariable("ADMIN_DESTRUCTIVE_ENABLED");
    if (!string.Equals(gate, "true", System.StringComparison.OrdinalIgnoreCase))
    {
        return Results.StatusCode(403);
    }
    Console.WriteLine("[ADMIN] /admin/wipe invoked — exiting process. K8s will restart this pod.");
    // Defer the exit slightly so the HTTP response gets flushed before
    // the kestrel listener tears down. Without this the caller sees
    // "connection reset" instead of 200 OK.
    _ = Task.Run(async () =>
    {
        await Task.Delay(250);
        System.Environment.Exit(0);
    });
    return Results.Ok(new { wiped = true });
});

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

    // ── Etcd client for membership self-registration ──
    // ETCD_ENDPOINT comes from Helm. Example:
    //   "http://crossv9-etcd:2379"
    // If unset we skip registering the client → AgentLifeCycleService takes
    // its null-DI path and logs a one-line "etcd disabled" message.
    var etcdEndpoint = Environment.GetEnvironmentVariable("ETCD_ENDPOINT");
    if (!string.IsNullOrWhiteSpace(etcdEndpoint))
    {
        services.AddSingleton<dotnet_etcd.EtcdClient>(_ =>
        {
            // Use insecure credentials for plain HTTP (cluster-internal).
            // The dotnet-etcd library requires explicit Credentials when the
            // address doesn't carry a TLS hint; without this we hit the
            // "Unable to determine the TLS configuration of the channel"
            // error we saw in logs/agent_logs_setup_etcd_client_1.
            return new dotnet_etcd.EtcdClient(
                connectionString: etcdEndpoint,
                configureChannelOptions: opts =>
                {
                    opts.Credentials = Grpc.Core.ChannelCredentials.Insecure;
                });
        });
        services.AddSingleton<Agent.Services.Etcd.IEtcdClientService, Agent.Services.Etcd.EtcdClientService>();
        Console.WriteLine($"[Agent] etcd client configured: {etcdEndpoint}");
    }
    else
    {
        Console.WriteLine("[Agent] ETCD_ENDPOINT not set; running without etcd membership registration");
    }

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
            // RocksDB block caches are NATIVE memory (C++ malloc, outside .NET GC heap).
            // totalAvailableMemory only covers the managed heap. Native memory is ADDITIONAL.
            // We cap total native block caches to 25% of available memory so that
            // managed heap (metadata, chunk cache, runtime) + native doesn't exceed the
            // container's actual memory. Memtables add ~512MB native on top of this.
            long availMem = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
            long nativeBudgetMb = Math.Max(512, availMem * 25 / 100 / (1024 * 1024));
            long bucketBlockCacheMb = nativeBudgetMb * 80 / 100; // 80% of native budget
            long chunkBlockCacheMb = nativeBudgetMb * 20 / 100;  // 20% of native budget
            Console.WriteLine($"[Storage] Using RocksDB backend path={rocksPath}, native budget={nativeBudgetMb}MB, bucket block cache={bucketBlockCacheMb}MB, chunk block cache={chunkBlockCacheMb}MB");
            return new RocksDbStorageService(rocksPath, pgbuilder.ConnectionString,
                bucketBlockCacheMb: bucketBlockCacheMb, chunkBlockCacheMb: chunkBlockCacheMb);
        }

    var storageDir = Environment.GetEnvironmentVariable("CHUNK_STORAGE_DIR") ?? "/tmp/crossv9_chunks";
        Console.WriteLine($"[Storage] Using local backend dir={storageDir}");
        return new LocalFileStorageService(storageDir, pgbuilder.ConnectionString);
    });
}
