using Grpc.Core;
using GrpcServiceExample;

public class GreeterService : Greeter.GreeterBase
{
    private readonly ILogger<GreeterService> _logger;

    public GreeterService(ILogger<GreeterService> logger)
    {
        _logger = logger;
    }

    public override Task<HelloReply> SayHello(HelloRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Received request from {Name}", request.Name);

        if (string.IsNullOrEmpty(request.Name))
        {
            _logger.LogError("Invalid request: Name is empty");
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Name must not be empty"));
        }

        return Task.FromResult(new HelloReply{
            Message = "Hello " + request.Name
        });
    }
}