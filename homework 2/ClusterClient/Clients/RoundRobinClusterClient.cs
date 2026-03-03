using System;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients;

public class RoundRobinClusterClient(string[] replicaAddresses) : ClusterClientBase(replicaAddresses)
{
    private readonly ReplicaPerformanceTracker tracker = new();

    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        var stopwatch = Stopwatch.StartNew();

        var orderedReplicas = tracker.OrderByFastest(ReplicaAddresses);
        var remainingReplicas = orderedReplicas.Length;

        foreach (var replica in orderedReplicas)
        {
            var remainingTime = timeout - stopwatch.Elapsed;
            if (remainingTime <= TimeSpan.Zero)
                throw new TimeoutException("No replica responded within overall timeout");

            var timeoutPerRequest = remainingTime.Divide(remainingReplicas);
            remainingReplicas--;

            var webRequest = CreateRequest(replica + "?query=" + query);
            Log.InfoFormat($"Processing {webRequest.RequestUri}");

            var sw = Stopwatch.StartNew();
            var requestTask = ProcessRequestAsync(webRequest);

            var completed = await Task.WhenAny(requestTask, Task.Delay(timeoutPerRequest));

            if (completed != requestTask)
            {
                sw.Stop();
                tracker.ReportResult(replica, sw.ElapsedMilliseconds);
                continue;
            }

            try
            {
                var result = await requestTask;
                sw.Stop();
                tracker.ReportResult(replica, sw.ElapsedMilliseconds);
                return result;
            }
            catch (WebException)
            {
                sw.Stop();
                tracker.ReportResult(replica, sw.ElapsedMilliseconds);
            }
        }

        throw new TimeoutException("No replica responded");
    }

    protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
}