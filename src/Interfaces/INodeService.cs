
namespace Agent.Interfaces;

public interface INodeService
{
    public Task JoinNetwork(string bootstrap_node_ip);
    public Task BuildFingerTable();
}
