using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients;

public class ParallelClusterClient(string[] replicaAddresses) : ClusterClientBase(replicaAddresses)
{
    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        var cts = new CancellationTokenSource(timeout);

        var tasks = ReplicaAddresses
            .Select(address => CallReplicaAsync(address, query, cts.Token))
            .ToList();

        while (tasks.Count > 0)
        {
            var completed = await Task.WhenAny(tasks);

            if (completed.IsCompletedSuccessfully)
            {
                await cts.CancelAsync();
                return completed.Result;
            }

            tasks.Remove(completed);
        }

        throw new TimeoutException("No replica responded within the timeout");
    }

    private async Task<string> CallReplicaAsync(string address, string query, CancellationToken token)
    {
        var uri = $"{address}?query={query}";
        var request = CreateRequest(uri);

        await using (token.Register(() => request.Abort()))
        {
            return await ProcessRequestAsync(request);
        }
    }

    protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
}