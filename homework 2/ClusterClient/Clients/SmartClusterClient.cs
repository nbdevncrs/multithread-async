using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients;

public class SmartClusterClient(string[] replicaAddresses) : ClusterClientBase(replicaAddresses)
{
    private readonly ReplicaPerformanceTracker tracker = new();

    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        var stopwatch = Stopwatch.StartNew();

        var orderedReplicas = tracker.OrderByFastest(ReplicaAddresses);
        var remainingReplicas = orderedReplicas.Length;

        var inFlight = new List<(string replica, Task<string> task, Stopwatch sw)>();

        foreach (var replica in orderedReplicas)
        {
            var remainingTime = timeout - stopwatch.Elapsed;
            if (remainingTime <= TimeSpan.Zero)
                break;

            var timeoutPerRequest = remainingTime.Divide(remainingReplicas);
            remainingReplicas--;

            var request = CreateRequest(replica + "?query=" + query);
            Log.InfoFormat($"Processing {request.RequestUri}");

            var sw = Stopwatch.StartNew();
            var task = ProcessRequestAsync(request);

            inFlight.Add((replica, task, sw));
                
            var delayTask = Task.Delay(timeoutPerRequest);
            var anyResponseTask = Task.WhenAny(inFlight.Select(x => x.task));

            var completed = await Task.WhenAny(anyResponseTask, delayTask);

            if (completed != anyResponseTask) continue;
            {
                await anyResponseTask;
                    
                var winner = inFlight.First(x => x.task.IsCompleted);

                try
                {
                    var result = await winner.task;
                    winner.sw.Stop();
                    tracker.ReportResult(winner.replica, winner.sw.ElapsedMilliseconds);
                    return result;
                }
                catch (WebException)
                {
                    winner.sw.Stop();
                    tracker.ReportResult(winner.replica, winner.sw.ElapsedMilliseconds);
                    inFlight.Remove(winner);
                }
            }
        }
            
        while (inFlight.Count > 0)
        {
            var remainingTime = timeout - stopwatch.Elapsed;
            if (remainingTime <= TimeSpan.Zero)
                break;

            var completed = await Task.WhenAny(
                Task.WhenAny(inFlight.Select(x => x.task)),
                Task.Delay(remainingTime)
            );

            if (completed is Task<string>)
            {
                var winner = inFlight.First(x => x.task.IsCompleted);

                try
                {
                    var result = await winner.task;
                    winner.sw.Stop();
                    tracker.ReportResult(winner.replica, winner.sw.ElapsedMilliseconds);
                    return result;
                }
                catch (WebException)
                {
                    winner.sw.Stop();
                    tracker.ReportResult(winner.replica, winner.sw.ElapsedMilliseconds);
                    inFlight.Remove(winner);
                }
            }
        }

        throw new TimeoutException("No replica responded within timeout");
    }

    protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
}