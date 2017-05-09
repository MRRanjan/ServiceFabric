using System.Collections.Generic;
using System.Web.Http;

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

        // Holds the votes and counts. NOTE: THIS IS NOT THREAD SAFE - JUST FOR THE PURPOSES OF THE LAB.
        static Dictionary<string, int> _counts = new Dictionary<string, int>();

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
                string path = string.Format(@"..\VotingServicePkg.Code.1.0.0\{0}", file);
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
