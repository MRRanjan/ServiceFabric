using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Net.Http.Headers;
using System.Web.Http;
using System.Threading.Tasks;

namespace VotingState.Controllers
{
    public sealed class StatefulVoteController : ApiController
    {
        const string activityHeader = "activity-id";

        // Keep an instance of the service.
        private IVotingService _service = null;

        // Controller constructor taking a IVotingService instance.
        // This is cheap dependency injection done in the listener.
        // You can also use your favorite DI framework.
        public StatefulVoteController(IVotingService vs)
        {
            _service = vs;
        }

        // GET api/votes 
        [HttpGet]
        [Route("api/votes")]
        public async Task<HttpResponseMessage> GetAsync()
        {
            string activityId = GetHeaderValueOrDefault(Request, activityHeader, () => { return Guid.NewGuid().ToString(); });
            ServiceEventSource.Current.ServiceRequestStart("VotesController.Get", activityId);

            IReadOnlyList<VotingData> counts = await _service.GetVotingDataAsync(activityId, CancellationToken.None);

            List<KeyValuePair<string, int>> votes = new List<KeyValuePair<string, int>>(counts.Count);
            foreach (VotingData data in counts)
            {
                votes.Add(new KeyValuePair<string, int>(data.Name, data.Count));
            }

            var response = Request.CreateResponse(HttpStatusCode.OK, votes);
            response.Headers.CacheControl = new CacheControlHeaderValue() { NoCache = true, MustRevalidate = true };

            ServiceEventSource.Current.ServiceRequestStop("VotesController.Get", activityId);
            _service.RequestCount = 1;
            return response;
        }

        [HttpPost]
        [Route("api/{key}")]
        public async Task<HttpResponseMessage> PostAsync(string key)
        {
            string activityId = GetHeaderValueOrDefault(Request, activityHeader, () => { return Guid.NewGuid().ToString(); });
            ServiceEventSource.Current.ServiceRequestStart("VotesController.Post", activityId);

            // Update or add the item.
            await _service.AddVoteAsync(key, 1, activityId, CancellationToken.None);

            ServiceEventSource.Current.ServiceRequestStop("VotesController.Post", activityId);
            _service.RequestCount = 1;
            return Request.CreateResponse(HttpStatusCode.NoContent);
        }

        /// <summary>
        /// Gets a value from a header collection or returns the default value from the function.
        /// </summary>
        public static string GetHeaderValueOrDefault(HttpRequestMessage request, string headerName, Func<string> getDefault)
        {
            // If headers are not specified, return the default string.
            if ((null == request) || (null == request.Headers))
                return getDefault();

            // Search for the header name in the list of headers.
            IEnumerable<string> values;
            if (true == request.Headers.TryGetValues(headerName, out values))
            {
                // Return the first value from the list.
                foreach (string value in values)
                    return value;
            }

            // return an empty string as default.
            return getDefault();
        }
    }
}
