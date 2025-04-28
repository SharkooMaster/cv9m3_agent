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

// IMPORTANT
AgnetaHandler.disabled = true;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddLogging( logging => 
{
    logging.AddConsole();
    logging.SetMinimumLevel(LogLevel.Debug);
});


builder.Services.AddGrpc(options => {
    options.MaxReceiveMessageSize = 1000 * 1024 * 1024;
    options.MaxSendMessageSize = 1000 * 1024 * 1024;
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
var networkFileSystemService = app.Services.GetRequiredService<GcsSqlStorageService>(); // Drop in replacement for nfs with gcp
NetworkFileStorageHandler.SetInstance(networkFileSystemService);

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// TODO: Uncomment for production
// app.UseHttpsRedirection();

app.UseRouting();

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

app.MapGet("/", () =>{ return "Hello world"; });
app.MapGet("/health", () =>{ return "true"; });

AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
{
    Console.WriteLine($"[UNHANDLED EXCEPTION]: {eventArgs.ExceptionObject}");
};

TaskScheduler.UnobservedTaskException += (sender, e) =>
{
    Console.WriteLine($"[UNOBSERVED TASK EXCEPTION]: {e.Exception}");
    e.SetObserved();
};

try
{
    // Wait for bootstrap
    while (!Globals.bootstraped)
    {
        Console.WriteLine($"Awaiting bootstrap completion: {Globals.bootstrap_node}");
        await Task.Delay(1000);
    }

    // Then properly await JoinNetwork
    Globals._NODE = await NodeService.JoinNetwork(Globals._NODE, Globals.bootstrap_node);

    Console.WriteLine($"Successfully joined network. My ID = {Globals._NODE.id}, IP = {Globals._NODE.ip}");
}
catch (Exception ex)
{
    Console.WriteLine($"Fatal error during startup: {ex.Message}");
    throw; // Let program crash if it cannot join
}


PushoverHandler.PushNotification($"Agent:{Globals.ETCD_ID}: Running");

await app.RunAsync();

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
        Database = "compressiondb",
        SslMode = SslMode.Disable,
        Pooling = true,
        MinPoolSize = 1,
        MaxPoolSize = 20,
        Timeout = 15,
        CommandTimeout = 30
    };

    services.AddSingleton<GcsSqlStorageService>(
        new GcsSqlStorageService(
            "cross-global-chunks", 
            pgbuilder.ConnectionString
        )
    );
}
