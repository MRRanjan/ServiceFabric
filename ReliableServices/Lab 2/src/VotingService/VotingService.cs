﻿using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

#if STATE
using System.Net;
using System.Text;
#endif

#if ETW
using System.Fabric.Health;
#endif

#if CONFIGUPGRADE
using System.Fabric.Description;
#endif

namespace VotingService
{
    /// <summary>
    /// The FabricRuntime creates an instance of this class for each service type instance. 
    /// </summary>
    internal sealed class VotingService : StatelessService
    {

#if ETW
        private TimeSpan _interval = TimeSpan.FromSeconds(30);
        private long _lastCount = 0L;
        private DateTime _lastReport = DateTime.UtcNow;
        private Timer _healthTimer = null;
        private FabricClient _client = null;
#endif
        public VotingService(StatelessServiceContext context)
            : base(context)
        {
#if ETW
            // Create the timer here, so we can do a change operation on it later, 
            // avoiding creating/disposing of the timer.
            _healthTimer = new Timer(ReportHealthAndLoad, null, Timeout.Infinite, Timeout.Infinite);

            //ICodePackageActivationContext ctx = context.CodePackageActivationContext;
#endif

#if CONFIGUPGRADE
            context.CodePackageActivationContext.ConfigurationPackageModifiedEvent 
                += CodePackageActivationContext_ConfigurationPackageModifiedEvent;
#endif
        }


        /// <summary>
        /// Optional override to create listeners (like tcp, http) for this service instance.
        /// </summary>
        /// <returns>The collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new ServiceInstanceListener[]
            {
                new ServiceInstanceListener(
                    serviceContext => new OwinCommunicationListener(
                        Startup.ConfigureApp, serviceContext, ServiceEventSource.Current, "ServiceEndpoint"))
            };
        }

#if STATE1
        private async Task ProcessRequestAsync(HttpListenerContext context, CancellationToken ct)
        {
            String output = null;
            try
            {
                // Grab the vote item string from a "Vote=" query string parameter
                HttpListenerRequest request = context.Request;
                String voteItem = request.QueryString["Vote"];
                if (voteItem != null)
                {
                    // TODO: Here, write code to perform the following steps:
                    // Hint: See the RunAsync method to help you with these steps.

                    // 1. Get a reference to a reliable dictionary using the 
                    //    inherited StateManager. The dictionary should String keys
                    //    and int values; Name the dictionary “Votes”

                    // 2. Create a new transaction using the inherited StateManager

                    // 3. Add the voteItem (with a count of 1) if it doesn’t already
                    //    exist or increment its count if it does exist.

                    // The code below prepares the HTML response. It gets all the current
                    // vote items (and counts) and separates each with a break (<br>)
                    var q = from kvp in voteDictionary.CreateEnumerable()
                                //orderby kvp.Key    // Intentionally commented out
                            select $"Item={kvp.Key}, Votes={kvp.Value}";
                    output = String.Join("<br>", q);
                }
            }
            catch (Exception ex) { output = ex.ToString(); }
            // Write response to client:
            using (var response = context.Response)
            {
                if (output != null)
                {
                    Byte[] outBytes = Encoding.UTF8.GetBytes(output);
                    response.OutputStream.Write(outBytes, 0, outBytes.Length);
                }
            }
        }
#endif

#if ETW
        protected override Task OnOpenAsync(CancellationToken cancellationToken)
        {

#if CONFIGUPGRADE
            // Force a call to LoadConfiguration because we missed the first event callback.
            LoadConfiguration();
            _client = new FabricClient();
#else
            _client = new FabricClient();
            _healthTimer = new Timer(ReportHealthAndLoad, null, _interval, _interval);
#endif
            return base.OnOpenAsync(cancellationToken);

        }

        public void ReportHealthAndLoad(object notused)
        {
            // Calculate the values and then remember current values for the next report.
            long total = Controllers.VotesController._requestCount;
            long diff = total - _lastCount;
            long duration = Math.Max((long)DateTime.UtcNow.Subtract(_lastReport).TotalSeconds, 1L);
            long rps = diff / duration;
            _lastCount = total;
            _lastReport = DateTime.UtcNow;

            // Create the health information for this instance of the service and send report to Service Fabric.
            HealthInformation hi = new HealthInformation("VotingServiceHealth", "Heartbeat", HealthState.Ok)
            {
                TimeToLive = _interval.Add(_interval),
                Description = $"{diff} requests since last report. RPS: {rps} Total requests: {total}.",
                RemoveWhenExpired = false,
                SequenceNumber = HealthInformation.AutoSequenceNumber
            };
            var sshr = new StatelessServiceInstanceHealthReport(Context.PartitionId, Context.InstanceId, hi);
            _client.HealthManager.ReportHealth(sshr);

            // Report the load
            Partition.ReportLoad(new[] { new LoadMetric("RPS", (int)rps) });

#if CONFIGUPGRADE
            // Log the health report.
            ServiceEventSource.Current.HealthReport(hi.SourceId, hi.Property, Enum.GetName(typeof(HealthState), hi.HealthState), 
                Context.PartitionId, Context.ReplicaOrInstanceId, hi.Description);
#endif

#if ROLLBACK
            // Report failing health report to cause rollback.
            var nodeList = _client.QueryManager.GetNodeListAsync(Context.NodeContext.NodeName).GetAwaiter().GetResult();
            var node = nodeList[0];
            if ("4" == node.UpgradeDomain || "3" == node.UpgradeDomain || "2" == node.UpgradeDomain)
            {
                hi = new HealthInformation("VotingServiceHealth", "Heartbeat", HealthState.Error);
                hi.TimeToLive = _interval.Add(_interval);
                hi.Description = $"Bogus health error to force rollback.";
                hi.RemoveWhenExpired = true;
                hi.SequenceNumber = HealthInformation.AutoSequenceNumber;
                sshr = new StatelessServiceInstanceHealthReport(Context.PartitionId, Context.InstanceId, hi);
                _client.HealthManager.ReportHealth(sshr);
            }
#endif
        }
#endif

#if CONFIGUPGRADE
        private void CodePackageActivationContext_ConfigurationPackageModifiedEvent(object sender, PackageModifiedEventArgs<ConfigurationPackage> e)
        {
            ServiceEventSource.Current.Message("CodePackageActivationContext_ConfigurationPackageModifiedEvent");
            LoadConfiguration();
        }

        private void LoadConfiguration()
        {
            // Get the Health Check Interval configuration value.
            ConfigurationPackage pkg = Context.CodePackageActivationContext.GetConfigurationPackageObject("Config");
            if (null != pkg)
            {
                if (true == pkg.Settings?.Sections?.Contains("Health"))
                {
                    ConfigurationSection settings = pkg.Settings.Sections["Health"];
                    if (true == settings?.Parameters.Contains("HealthCheckIntervalSeconds"))
                    {
                        long lValue = 0;
                        ConfigurationProperty prop = settings.Parameters["HealthCheckIntervalSeconds"];
                        if (long.TryParse(prop?.Value, out lValue))
                        {
                            _interval = TimeSpan.FromSeconds(Math.Max(30, lValue));
                            _healthTimer.Change(_interval, _interval);
                            //_healthTimer?.Dispose();
                            //_healthTimer = new Timer(ReportHealthAndLoad, null, _interval, _interval);
                        }

                        ServiceEventSource.Current.HealthReportIntervalChanged("VotingServiceHealth", "IntervalChanged", 
                            Context.PartitionId, Context.ReplicaOrInstanceId, (int)_interval.TotalSeconds);
                    }
                }
            }
        }
#endif

    }
}
