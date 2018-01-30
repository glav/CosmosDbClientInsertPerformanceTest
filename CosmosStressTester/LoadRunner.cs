using DocDbClient;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CosmosStressTester
{
    internal class LoadRunner
    {
        DocDbClient.CosmosWriteOperations<PerfTestDto> _dbClient;
        List<Task> _tasks = new List<Task>();
        List<Thread> _threads = new List<Thread>();
        Random _rnd = new Random(DateTime.Now.Millisecond);
        int _records = Config.Records;
        long _recordCount = 0;
        public LoadRunner(DocDbClient.CosmosWriteOperations<PerfTestDto> dbClient)
        {
            _dbClient = dbClient;
        }

        public async Task Run()
        {
            Logger.Info($"Starting a load run with a concurrency of {Config.Concurrency} and number of records: {_records}");
            var watch = new Stopwatch();
            // prepare
            watch.Start();

            // Task based execution
            for (var thrd = 0; thrd < Config.Concurrency; thrd++)
            {
                _tasks.Add(WriteDto());
            }
            await Task.WhenAll(_tasks);

            // run
            watch.Stop();

            // done
            Logger.Info("Load run complete.");

            var output = new StringBuilder();
            var timeTaken = $"Time elapsed: {watch.Elapsed.Hours}:{watch.Elapsed.Minutes}:{watch.Elapsed.Seconds}.{watch.Elapsed.Milliseconds}";
            Logger.Info(timeTaken);
            output.AppendLine($"Run: Concurrency: {Config.Concurrency}, Records per concurrent task: {Config.Records}");
            output.AppendLine(timeTaken);

            var throughput = Math.Round(_recordCount / watch.Elapsed.TotalSeconds);
            var throughputText = $"Avg throughput: {throughput} inserts per second";
            Logger.Info(throughputText);
            output.AppendLine(throughputText);

            System.IO.File.WriteAllText("RunResults.txt", output.ToString());

        }


        private async Task WriteDto()
        {
            var storeNumber = System.Threading.Thread.CurrentThread.ManagedThreadId;
            var articleNumber = _rnd.Next(1000, 100000).ToString();
            var stockOnHand = _rnd.Next(0, 100);
            var uri = UriFactory.CreateDocumentCollectionUri(Config.DataLocation.DatabaseId, Config.DataLocation.CollectionId);
            for (var cnt = 0; cnt < _records; cnt++)
            {
                //await DocumentOperationHelper.DoOperationWithRetryAsync(() =>
                await _dbClient.CreateDocumentInCollection(uri,
                       new PerfTestDto
                       {
                           KeyNumber = storeNumber,
                           SomeText = articleNumber,
                           SomeCount = stockOnHand
                       });
                            //, 10);
                _recordCount++;
                if (_recordCount % 10000 == 0)
                {
                    Logger.Info($"{_recordCount} records written");
                }
            }
        }
    }
}
