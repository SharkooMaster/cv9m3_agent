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

app.Run();

void ConfigureServices(IServiceCollection services)
{
    Console.WriteLine("Initiating iAgneta");
    services.AddSingleton<AgnetaClientService>(new AgnetaClientService("wss://agneta-loadbalancer.default.svc.cluster.local/log/ws"));
    Console.WriteLine("Connected iAgneta");
}
