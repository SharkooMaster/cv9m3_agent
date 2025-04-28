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

while(!Globals.bootstraped)
{
    Console.WriteLine($"Waiting for bootstrap");
}
Globals._NODE = await NodeService.JoinNetwork(Globals._NODE, Globals.bootstrap_node);
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
