using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.Data;

namespace VotingState
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class VotingState : StatefulService, IVotingService
    {
        public VotingState(StatefulServiceContext context, InitializationCallbackAdapter adapter)
            : base(context, new ReliableStateManager(context, 
                new ReliableStateManagerConfiguration(onInitializeStateSerializersEvent: adapter.OnInitialize)))
        {
            adapter.StateManager = this.StateManager;
        }


        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[]
            {
                new ServiceReplicaListener(serviceContext => new OwinCommunicationListener(
                    serviceContext, this, ServiceEventSource.Current,   "ServiceEndpoint"))
            };

        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }


            //var myDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("myDictionary");

            //while (true)
            //{
            //    cancellationToken.ThrowIfCancellationRequested();

            //    using (var tx = this.StateManager.CreateTransaction())
            //    {
            //        var result = await myDictionary.TryGetValueAsync(tx, "Counter");

            //        ServiceEventSource.Current.ServiceMessage(this.Context, "Current Counter Value: {0}",
            //            result.HasValue ? result.Value.ToString() : "Value does not exist.");

            //        await myDictionary.AddOrUpdateAsync(tx, "Counter", 0, (key, value) => ++value);

            //        // If an exception is thrown before calling CommitAsync, the transaction aborts, all changes are 
            //        // discarded, and nothing is saved to the secondary replicas.
            //        await tx.CommitAsync();
            //    }

            //    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            //}
        }

        /// <summary>
        /// Gets the count dictionary. If it doesn't already exist it is created.
        /// </summary>
        /// <remarks>This is a good example of how to initialize reliable collections. 
        /// Rather than initializing and caching a value, this approach will work on 
        /// both primary and secondary replicas (if secondary reads are enabled.</remarks>
        private async Task<IReliableDictionary<string, VotingData>> GetCountDictionaryAsync()
        {
            return await StateManager.GetOrAddAsync<IReliableDictionary<string, VotingData>>("votingCountDictionary").ConfigureAwait(false);
        }

        // Track the number of requests to the controller.
        private long _requestCount = 0;

        // Returns or increments the request count.
        public long RequestCount
        {
            get { return Volatile.Read(ref _requestCount); }
            set { Interlocked.Increment(ref _requestCount); }
        }

        /// <summary>
        /// Gets the list of VotingData items.
        /// </summary>
        public async Task<IReadOnlyList<VotingData>> GetVotingDataAsync(string id, CancellationToken token)
        {
            List<VotingData> items = new List<VotingData>();
            ServiceEventSource.Current.ServiceRequestStart("VotingState.GetVotingDataAsync", id);

            // Get the dictionary.
            var dictionary = await GetCountDictionaryAsync();
            using (ITransaction tx = StateManager.CreateTransaction())
            {
                // Create the enumerable and get the enumerator.
                var enumItems = (await dictionary.CreateEnumerableAsync(tx)).GetAsyncEnumerator();
                while (await enumItems.MoveNextAsync(token))
                {
                    items.Add(enumItems.Current.Value);
                    if (items.Count > 1000)
                        break;
                }
            }

            return items;
        }

        /// <summary>
        /// Updates the count on a vote or adds a new vote if it doesn't already exist.
        /// </summary>
        public async Task AddVoteAsync(string key, int count, string id, CancellationToken token)
        {
            ServiceEventSource.Current.ServiceRequestStart("VotingState.AddVoteAsync", id);

            // Get the dictionary.
            var dictionary = await GetCountDictionaryAsync();
            using (ITransaction tx = StateManager.CreateTransaction())
            {
                // Try to get the existing value
                ConditionalValue<VotingData> result = await dictionary.TryGetValueAsync(tx, key, LockMode.Update);
                if (result.HasValue)
                {
                    // VotingData is immutable to ensure the reference returned from the dictionary
                    // isn't modified. If it were not immutable, you changed a field’s value, and then the transaction aborts, the value will 
                    // remain modified in memory, corrupting your data.
                    VotingData newData = result.Value.UpdateWith(result.Value.Count + count);
                    await dictionary.TryUpdateAsync(tx, key, newData, result.Value);
                }
                else
                {
                    // Add a new VotingData item to the collection
                    await dictionary.AddAsync(tx, key, new VotingData(key, count, count, DateTimeOffset.Now));
                }

                // Commit the transaction.
                await tx.CommitAsync();
            }
        }

    }
}
