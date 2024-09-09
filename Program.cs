using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddGrpc();

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

//app.UseHttpsRedirection();
app.UseRouting();

app.MapGrpcService<GreeterService>();
app.MapGet("/", () =>
{
    return "Hello world";
});

app.Run();
