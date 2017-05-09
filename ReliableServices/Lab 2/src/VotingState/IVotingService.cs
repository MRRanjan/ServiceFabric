using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace VotingState
{
    // Voting Service Operations interface.
    // For lab purposes, the operations are included 
    // in the service class. They should be separated
    // out to allow for unit testing.
    public interface IVotingService
    {
        // Gets the list of VotingData structures.
        Task<IReadOnlyList<VotingData>> GetVotingDataAsync(string id, CancellationToken token);

        // Updates the count on a vote or adds a new vote if it doesn't already exist.
        Task AddVoteAsync(string key, int count, string id, CancellationToken token);

        /// <summary>
        /// Tracks the number of requests to the service across all controller instances.
        /// </summary>
        long RequestCount { get; set; }
    }
}
