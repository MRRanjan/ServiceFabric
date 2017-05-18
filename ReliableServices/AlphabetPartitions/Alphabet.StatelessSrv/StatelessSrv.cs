using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using System.Diagnostics;
using System.Fabric.Description;
using System.Net;
using System.Text;

namespace Alphabet.StatelessSrv
{
    /// <summary>
    /// An instance of this class is created for each service instance by the Service Fabric runtime.
    /// </summary>
    internal sealed class StatelessSrv : StatelessService
    {
        string instanceId = "";
        int instanceCount = 0;
        long iterations = 0;

        public StatelessSrv(StatelessServiceContext context)
            : base(context)
        {
            instanceId = $"{Process.GetCurrentProcess().Id}#{context.InstanceId}";
            ServiceEventSource.Current.Message($"Alphabet.StatelessSrv.StatelessSrv.Ctor: {instanceId} / {++instanceCount}");
        }

        /// <summary>
        /// Optional override to create listeners (e.g., TCP, HTTP) for this service replica to handle client or user requests.
        /// </summary>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            ServiceEventSource.Current.Message($"Alphabet.StatelessSrv.StatelessSrv.CreateServiceReplicaListeners: {instanceId} / {++instanceCount}");
            return new[] { new ServiceInstanceListener(context => this.CreateInternalListener(context)) };
            //return new ServiceInstanceListener[0];
        }

        private ICommunicationListener CreateInternalListener(ServiceContext context)
        {
            ServiceEventSource.Current.Message($"Alphabet.StatelessSrv.StatelessSrv.CreateInternalListener: {instanceId} / {++instanceCount}");
            
            EndpointResourceDescription internalEndpoint = context.CodePackageActivationContext.GetEndpoint("StatelessSrvEndpoint");

            string uriPrefix = String.Format(
                "{0}://+:{1}/{2}/{3}/",
                internalEndpoint.Protocol,
                internalEndpoint.Port,
                context.PartitionId,
                context.ReplicaOrInstanceId);

            string nodeIP = FabricRuntime.GetNodeContext().IPAddressOrFQDN;

            // The published URL is slightly different from the listening URL prefix.
            // The listening URL is given to HttpListener.
            // The published URL is the URL that is published to the Service Fabric Naming Service,
            // which is used for service discovery. Clients will ask for this address through that discovery service.
            // The address that clients get needs to have the actual IP or FQDN of the node in order to connect,
            // so we need to replace '+' with the node's IP or FQDN.
            string uriPublished = uriPrefix.Replace("+", nodeIP);
            return new HttpCommunicationListener(uriPrefix, uriPublished, this.ProcessInternalRequest);
        }

        private async Task ProcessInternalRequest(HttpListenerContext context, CancellationToken cancelRequest)
        {
            ServiceEventSource.Current.Message($"Alphabet.StatelessSrv.StatelessSrv.ProcessInternalRequest: {instanceId} / {++instanceCount}");
            string output = null;
            string user = context.Request.QueryString["lastname"].ToString();

            try
            {
                output = $"<h4>StatelessSrv.ProcessInternalRequest:</h4> {instanceId} / {instanceCount}<br>";
                output = output + $"Response ({iterations}) > {Reverse(user)}";
            }
            catch (Exception ex)
            {
                output = ex.Message;
            }

            using (HttpListenerResponse response = context.Response)
            {
                if (output != null)
                {
                    byte[] outBytes = Encoding.UTF8.GetBytes(output);
                    response.OutputStream.Write(outBytes, 0, outBytes.Length);
                }
            }
        }

        public static string Reverse(string s)
        {
            char[] charArray = s.ToCharArray();
            Array.Reverse(charArray);
            return new string(charArray);
        }

        /// <summary>
        /// This is the main entry point for your service instance.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service instance.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ++iterations;               
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }
}
