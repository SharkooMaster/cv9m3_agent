using Agent.Utils.Misc;
using Agent.Services.Agneta;
using Agent.Models.Misc;

namespace Agent.Interfaces.Agneta
{
    public interface IAgnetaClientService
    {
        Task<NeighbourData> GetAssignedNeighbour();
        Task SendUsageStatistics();

        Task ConnectAsync();
        Task SendMessageAsync(string message);
        Task<string> RecieveMessageAsync();
        Task SendCloseAsync();
    }
}