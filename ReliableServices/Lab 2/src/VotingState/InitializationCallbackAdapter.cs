using Microsoft.ServiceFabric.Data;
using System;
using System.Threading.Tasks;

namespace VotingState
{
    // Enables configuration of the state manager.
    public sealed class InitializationCallbackAdapter
    {
        [Obsolete("This method uses a method that is marked as obsolete.", false)]
        public Task OnInitialize()
        {
            // This is marked obsolete, but is supported. This interface is likely
            // to change in the future.
            var serializer = new VotingDataSerializer();
            this.StateManager.TryAddStateSerializer(serializer);
            return Task.FromResult(true);
        }

        public IReliableStateManager StateManager { get; set; }
    }
}
