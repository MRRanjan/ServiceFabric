using System;
using System.Fabric;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Owin.Hosting;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Owin;
using System.Web.Http.Controllers;
using System.Net.Http;
using System.Reflection;
using System.Web.Http.Dispatcher;
using System.Web.Http;

namespace VotingState
{
    internal class OwinCommunicationListener : ICommunicationListener, IHttpControllerActivator
    {
        private readonly ServiceEventSource eventSource;
        private readonly StatefulServiceContext serviceContext;
        private readonly string endpointName;
        private readonly string appRoot;

        private IDisposable webApp;
        private string publishAddress;
        private string listeningAddress;
        private readonly IVotingService serviceInstance;

        public OwinCommunicationListener(StatefulServiceContext serviceContext,
                    IVotingService svc, ServiceEventSource eventSource, string endpointName)
            : this(serviceContext, svc, eventSource, endpointName, null)
        {
        }

        public OwinCommunicationListener(StatefulServiceContext serviceContext,
                    IVotingService svc, ServiceEventSource eventSource, string endpointName, string appRoot)
        {
            if (serviceContext == null) throw new ArgumentNullException(nameof(serviceContext));
            if (endpointName == null) throw new ArgumentNullException(nameof(endpointName));
            if (eventSource == null) throw new ArgumentNullException(nameof(eventSource));
            if (null == svc) throw new ArgumentNullException(nameof(svc));

            this.serviceInstance = svc;
            this.serviceContext = serviceContext;
            this.endpointName = endpointName;
            this.eventSource = eventSource;
            this.appRoot = appRoot;
        }

        public bool ListenOnSecondary { get; set; }

        public Task<string> OpenAsync(CancellationToken cancellationToken)
        {
            var serviceEndpoint = this.serviceContext.CodePackageActivationContext.GetEndpoint(this.endpointName);
            int port = serviceEndpoint.Port;

            if (this.serviceContext is StatefulServiceContext)
            {
                StatefulServiceContext statefulServiceContext = this.serviceContext as StatefulServiceContext;

                this.listeningAddress = string.Format(
                    CultureInfo.InvariantCulture,
                    "http://+:{0}/{1}{2}/{3}/{4}",
                    port,
                    string.IsNullOrWhiteSpace(this.appRoot)
                        ? string.Empty
                        : this.appRoot.TrimEnd('/') + '/',
                    statefulServiceContext.PartitionId,
                    statefulServiceContext.ReplicaId,
                    Guid.NewGuid());
            }
            else
            {
                throw new InvalidOperationException();
            }

            this.publishAddress = this.listeningAddress.Replace("+", FabricRuntime.GetNodeContext().IPAddressOrFQDN);

            try
            {
                this.eventSource.ServiceMessage(this.serviceContext, "Starting web server on " + this.listeningAddress);
                this.webApp = WebApp.Start(this.listeningAddress, ConfigureApp);
                this.eventSource.ServiceMessage(this.serviceContext, "Listening on " + this.publishAddress);
                return Task.FromResult(this.publishAddress);
            }
            catch (Exception ex)
            {
                this.eventSource.ServiceMessage(this.serviceContext, "Web server failed to open. " + ex.ToString());
                this.StopWebServer();
                throw;
            }
        }

        public Task CloseAsync(CancellationToken cancellationToken)
        {
            this.eventSource.ServiceMessage(this.serviceContext, "Closing web server");
            this.StopWebServer();
            return Task.FromResult(true);
        }

        public void Abort()
        {
            this.eventSource.ServiceMessage(this.serviceContext, "Aborting web server");
            this.StopWebServer();
        }

        private void StopWebServer()
        {
            if (this.webApp != null)
            {
                try
                {
                    this.webApp.Dispose();
                }
                catch (ObjectDisposedException)
                {
                    // no-op
                }
            }
        }

        // This code configures Web API. This is called from Web.Start.
        private void ConfigureApp(IAppBuilder appBuilder)
        {
            // Configure Web API for self-host. 
            HttpConfiguration config = new HttpConfiguration();

            config.MapHttpAttributeRoutes();

            // Replace the default controller activator (to support optional injection of the stateless service into the controllers)
            config.Services.Replace(typeof(IHttpControllerActivator), this);

            config.Routes.MapHttpRoute(
                name: "DefaultApi",
                routeTemplate: "api/{controller}/{id}",
                defaults: new { id = RouteParameter.Optional }
            );

            appBuilder.UseWebApi(config);

        }

        /// <summary>
        /// Called to activate an instance of HTTP controller in the WebAPI pipeline
        /// </summary>
        /// <param name="request">HTTP request that triggered</param>
        /// <param name="controllerDescriptor">Description of the controller that was selected</param>
        /// <param name="controllerType">The type of the controller that was selected for this request</param>
        /// <returns>An instance of the selected HTTP controller</returns>
        /// <remarks>This is a cheap way to avoid a framework such as Unity. 
        /// If already using Unity, that is a better approach.</remarks>
        public IHttpController Create(HttpRequestMessage request, HttpControllerDescriptor controllerDescriptor, Type controllerType)
        {
            // If the controller defines a constructor with a single parameter of the type which implements the service type, create a new instance and inject this._serviceInstance
            ConstructorInfo ci = controllerType.GetConstructor(BindingFlags.Instance | BindingFlags.Public, null, CallingConventions.HasThis, new[] { typeof(IVotingService) }, new ParameterModifier[0]);
            if (null != ci)
            {
                object[] args = new object[1] { serviceInstance };
                return ci.Invoke(args) as IHttpController;
            }

            // If no matching constructor was found, just call the default parameter-less constructor 
            return Activator.CreateInstance(controllerDescriptor.ControllerType) as IHttpController;
        }
    }
}
