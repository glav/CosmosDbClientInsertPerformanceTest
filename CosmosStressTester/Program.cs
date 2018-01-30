using DocDbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosStressTester
{
    class Program
    {
        private static DocDbClient.CosmosWriteOperations<PerfTestDto> _dbClient;
        static void Main(string[] args)
        {
            Logger.Info("CosmosDb Perf Stress Tester");
            Logger.Info("Initialising...");
            try
            {
                _dbClient = new DocDbClient.CosmosWriteOperations<PerfTestDto>(Config.DataLocation, Config.CosmosConfiguration);
                _dbClient.CreateDocumentCollectionIfNotExists().Wait();
            } catch (Exception ex)
            {
                Logger.Error($"Error initialising: {ex.Message}");
                return;
            }
            Logger.Info("Initialised.");

            var loadRunner = new LoadRunner(_dbClient);
            loadRunner.Run().Wait();
        }
    }
}
