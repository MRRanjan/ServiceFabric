using System.Collections.Generic;
using System.Web.Http;

#if STATE
using System.Fabric;
using System.Threading.Tasks;
using Newtonsoft.Json;
#endif

namespace VotingService.Controllers
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Threading;
    using System.Net.Http.Headers;
    using System.Web.Http;

    public class VotesController : ApiController
    {
        // Used for health checks.
        public static long _requestCount = 0L;

        
#if STATE
        HttpClient _client = new HttpClient();
#else
        // Holds the votes and counts. NOTE: THIS IS NOT THREAD SAFE - JUST FOR THE PURPOSES OF THE LAB.
        static Dictionary<string, int> _counts = new Dictionary<string, int>();
#endif

#if STATE
        [HttpGet]
        [Route("api/votes")]
        public async Task<HttpResponseMessage> Get()
        {
            string activityId = Guid.NewGuid().ToString();
            ServiceEventSource.Current.ServiceRequestStart("VotesController.Get", activityId);

            Interlocked.Increment(ref _requestCount);

            string url = $"http://localhost:19081/Voting/VotingState/api/votes?PartitionKey=0&PartitionKind=Int64Range";
            HttpResponseMessage msg = await _client.GetAsync(url).ConfigureAwait(false);
            string json = await msg.Content.ReadAsStringAsync().ConfigureAwait(false);
            List<KeyValuePair<string, int>> votes = JsonConvert.DeserializeObject<List<KeyValuePair<string, int>>>(json);

            var response = Request.CreateResponse(HttpStatusCode.OK, votes);
            response.Headers.CacheControl = new CacheControlHeaderValue() { NoCache = true, MustRevalidate = true };

            ServiceEventSource.Current.ServiceRequestStop("VotesController.Get", activityId);
            return response;
        }

#else
        // GET api/votes 
        [HttpGet]
        [Route("api/votes")]
        public HttpResponseMessage Get()
        {
#if ETW
            string activityId = Guid.NewGuid().ToString();
            ServiceEventSource.Current.ServiceRequestStart("VotesController.Get", activityId);
#endif

            Interlocked.Increment(ref _requestCount);

            List<KeyValuePair<string, int>> votes = new List<KeyValuePair<string, int>>(_counts.Count);
            foreach (KeyValuePair<string, int> kvp in _counts)
            {
                votes.Add(kvp);
            }

            var response = Request.CreateResponse(HttpStatusCode.OK, votes);
            response.Headers.CacheControl = new CacheControlHeaderValue() { NoCache = true, MustRevalidate = true };

#if ETW
            ServiceEventSource.Current.ServiceRequestStop("VotesController.Get", activityId);
#endif
            return response;
        }
#endif

#if STATE
        [HttpPost]
        [Route("api/{key}")]
        public async Task<HttpResponseMessage> Post(string key)
        {
            string activityId = Guid.NewGuid().ToString();
            ServiceEventSource.Current.ServiceRequestStart("VotesController.Post", activityId);

            Interlocked.Increment(ref _requestCount);

            string url = $"http://localhost:19081/Voting/VotingState/api/{key}?PartitionKey=0&PartitionKind=Int64Range";
            HttpResponseMessage msg = await _client.PostAsync(url, null).ConfigureAwait(false);

            ServiceEventSource.Current.ServiceRequestStop("VotesController.Post", activityId);
            return Request.CreateResponse(msg.StatusCode);
        }
#else
        [HttpPost]
        [Route("api/{key}")]
        public HttpResponseMessage Post(string key)
        {
#if ETW
            string activityId = Guid.NewGuid().ToString();
            ServiceEventSource.Current.ServiceRequestStart("VotesController.Post", activityId);
#endif
            Interlocked.Increment(ref _requestCount);

            if (false == _counts.ContainsKey(key))
            {
                _counts.Add(key, 1);
            }
            else
            {
                _counts[key] = _counts[key] + 1;
            }
#if ETW
            ServiceEventSource.Current.ServiceRequestStop("VotesController.Post", activityId);
#endif

            return Request.CreateResponse(HttpStatusCode.NoContent);
        }
#endif

#if STATE
        [HttpDelete]
        [Route("api/{key}")]
        public HttpResponseMessage Delete(string key)
        {
            string activityId = Guid.NewGuid().ToString();
            ServiceEventSource.Current.ServiceRequestStart("VotesController.Delete", activityId);

            Interlocked.Increment(ref _requestCount);

            // Ignoring delete for this lab.

            ServiceEventSource.Current.ServiceRequestStop("VotesController.Delete", activityId);
            return Request.CreateResponse(HttpStatusCode.NotFound);
        }

#else
        [HttpDelete]
        [Route("api/{key}")]
        public HttpResponseMessage Delete(string key)
        {
#if ETW
            string activityId = Guid.NewGuid().ToString();
            ServiceEventSource.Current.ServiceRequestStart("VotesController.Delete", activityId);
#endif
            Interlocked.Increment(ref _requestCount);

            if (true == _counts.ContainsKey(key))
            {
                if (_counts.Remove(key))
                    return Request.CreateResponse(HttpStatusCode.OK);
            }

#if ETW
            ServiceEventSource.Current.ServiceRequestStop("VotesController.Delete", activityId);
#endif

            return Request.CreateResponse(HttpStatusCode.NotFound);
        }
#endif

        [HttpGet]
        [Route("api/{file}")]
        public HttpResponseMessage GetFile(string file)
        {
#if ETW
            string activityId = Guid.NewGuid().ToString();
            ServiceEventSource.Current.ServiceRequestStart("VotesController.GetFile", activityId);
#endif
            string response = null;
            string responseType = "text/html";

            Interlocked.Increment(ref _requestCount);

            // Validate file name.
            if ("index.html" == file)
            {
#if STATE
                string path = Path.Combine(FabricRuntime.GetActivationContext().GetCodePackageObject("Code").Path,"index.html");
#else
                // This hardcoded path is only for the lab. Later in the lab when the version is changed, this
                // hardcoded path must be changed to use the UX. In part 2 of the lab, this will be calculated
                // using the connected service path.
                string path = string.Format(@"..\VotingServicePkg.Code.1.0.5\{0}", file);                
#endif
                response = File.ReadAllText(path);
            }

#if ETW
            ServiceEventSource.Current.ServiceRequestStop("VotesController.GetFile", activityId);
#endif
            if (null != response)
                return Request.CreateResponse(HttpStatusCode.OK, response, responseType);
            else
                return Request.CreateErrorResponse(HttpStatusCode.NotFound, "File");
        }
    }

}
