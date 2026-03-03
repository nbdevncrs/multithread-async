using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

namespace ClusterClient.Clients
{
    public class ReplicaPerformanceTracker
    {
        private readonly ConcurrentDictionary<string, ReplicaStats> stats = new();

        public string[] OrderByFastest(string[] replicas)
        {
            return replicas
                .Select((r, i) => new { Replica = r, Index = i })
                .OrderBy(x =>
                    stats.TryGetValue(x.Replica, out var s) && s.HasSamples
                        ? s.AverageMs
                        : long.MaxValue)
                .ThenBy(x => x.Index)
                .Select(x => x.Replica)
                .ToArray();
        }

        public void ReportResult(string replica, long elapsedMs)
        {
            var stat = stats.GetOrAdd(replica, _ => new ReplicaStats());
            stat.AddSample(elapsedMs);
        }

        private class ReplicaStats
        {
            private long avgMs;
            private long count;

            public bool HasSamples => Volatile.Read(ref count) > 0;

            public long AverageMs
            {
                get
                {
                    var avg = Volatile.Read(ref avgMs);
                    return avg == 0 ? long.MaxValue : avg;
                }
            }

            public void AddSample(long sampleMs)
            {
                var newCount = Interlocked.Increment(ref count);

                long oldAvg, newAvg;
                do
                {
                    oldAvg = Volatile.Read(ref avgMs);
                    newAvg = (oldAvg * (newCount - 1) + sampleMs) / newCount;
                } while (Interlocked.CompareExchange(ref avgMs, newAvg, oldAvg) != oldAvg);
            }
        }
    }
}