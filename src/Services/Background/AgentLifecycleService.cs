
public class AgentLifeCycleService : BackgroundService
{
    private readonly ILogger<AgentLifeCycleService> _logger;
    public AgentLifeCycleService(ILogger<AgentLifeCycleService> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        throw new NotImplementedException();
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        return base.StartAsync(cancellationToken);
    }

}