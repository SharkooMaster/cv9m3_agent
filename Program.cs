using System.Net;
using System.Net.Security;
using Agent.Services;
using Agent.Services.Agneta;
using Agent.Services.Etcd;
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
    options.Listen(IPAddress.Any, 5000, o => o.Protocols = HttpProtocols.Http2);
    options.Listen(IPAddress.Any, 5001, o => o.Protocols = HttpProtocols.Http1);
    //options.ListenLocalhost(5000, o => o.Protocols = HttpProtocols.Http2);
    //options.ListenLocalhost(5001, o => o.Protocols = HttpProtocols.Http1AndHttp2);
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Background services
builder.Services.AddHostedService<AgentLifeCycleService>();
//builder.Services.AddHostedService<AgentRuntimeService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// TODO: Uncomment for production
// app.UseHttpsRedirection();

//app.UseHttpsRedirection();
app.UseRouting();

app.MapGrpcService<GreeterService>();
app.MapGet("/", () =>
{
    return "Hello world";
});

app.Run();

void ConfigureServices(IServiceCollection services)
{
    /*
    Console.WriteLine("Initiating iEtcd");
    services.AddSingleton<EtcdClient>(provider => {
        var configuration = provider.GetRequiredService<IConfiguration>();
        var etcdUrl = configuration["Etcd:Url"];
		Console.WriteLine($"etcd_url: {etcdUrl}");

		return new EtcdClient(etcdUrl, configureChannelOptions: (options => {
            options.Credentials = ChannelCredentials.Insecure;
        }));
    });
    services.AddSingleton<IEtcdClientService, EtcdClientService>();
	Console.WriteLine("Connected iEtcd");
    */
    Console.WriteLine("Initiating iAgneta");
    services.AddSingleton<IAgnetaClientService, AgnetaClientService>();
    Console.WriteLine("Connected iAgneta");
}
