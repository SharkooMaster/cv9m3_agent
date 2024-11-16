using System.Net;
using System.Net.Security;
using Agent.Services;
using Agent.Services.Agneta;
using Agent.Services.Etcd;
using Agent.Services.Grpc;
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

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddGrpc();

ConfigureServices(builder.Services);

// Configure Kestrel to allow HTTP/2 without TLS
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(5000, o => o.Protocols = HttpProtocols.Http2);
    options.ListenAnyIP(5001, o => o.Protocols = HttpProtocols.Http1);
    //options.ListenLocalhost(5000, o => o.Protocols = HttpProtocols.Http2);
    //options.ListenLocalhost(5001, o => o.Protocols = HttpProtocols.Http1AndHttp2);
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Background services
builder.Services.AddHostedService<AgentLifeCycleService>();
builder.Services.AddHostedService<AgentRuntimeService>();

var app = builder.Build();

var pushoverClientService = app.Services.GetRequiredService<PushoverClientService>();
PushoverHandler.SetInstance(pushoverClientService);

var agnetaClientService = app.Services.GetRequiredService<AgnetaClientService>();
await agnetaClientService.ConnectAsync();
AgnetaHandler.SetInstance(agnetaClientService);

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// TODO: Uncomment for production
// app.UseHttpsRedirection();

app.UseRouting();

app.MapGrpcService<GreeterService>();
app.MapGrpcService<QueryAgentService>();

app.MapGet("/", () =>{ return "Hello world"; });

PushoverHandler.PushNotification($"Gateway:{Globals.ETCD_ID}: Running");

app.Run();

await AgnetaHandler.Close();
void ConfigureServices(IServiceCollection services)
{
    services.AddSingleton<AgnetaClientService>(new AgnetaClientService("wss://agneta-loadbalancer.default.svc.cluster.local/log/ws"));
    services.AddSingleton<PushoverClientService>(new PushoverClientService());
}
