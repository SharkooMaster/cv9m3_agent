using System.Net;
using Agent.Services.Etcd;
using dotnet_etcd;
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
    services.AddSingleton<EtcdClient>(provider => {
        var configuration = provider.GetRequiredService<IConfiguration>();
        var etcdUrl = configuration["Etcd:Url"];
        return new EtcdClient(etcdUrl);
    });
    services.AddScoped<IEtcdClientService, EtcdClientService>();
}
