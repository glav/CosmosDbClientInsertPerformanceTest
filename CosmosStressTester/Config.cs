using DocDbClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosStressTester
{
    public static class Config
    {
        public static DbLocation DataLocation
        {
            get
            {
                return new DbLocation(ConfigurationManager.AppSettings["CosmosDb.CollectionId"], ConfigurationManager.AppSettings["CosmosDb.DatabaseId"]);
            }
        }

        public static CosmosDbConfiguration CosmosConfiguration
        {
            get
            {
                return CosmosDbConfiguration.CreateValidConfiguration(ConfigurationManager.AppSettings["CosmosDb.ReadModelEndpoint"],
                    ConfigurationManager.AppSettings["CosmosDb.ReadModelKey"],
                    ConfigurationManager.AppSettings["CosmosDb.Throughput"]);
            }
        }

        public static int Concurrency
        {
            get
            {
                int threads;
                if (int.TryParse(ConfigurationManager.AppSettings["Load.Concurrency"], out threads))
                {
                    return threads;
                }

                return 1;
            }
        }

        public static int Records
        {
            get
            {
                int records;
                if (int.TryParse(ConfigurationManager.AppSettings["Load.Records"], out records))
                {
                    return records;
                }

                return 1;
            }
        }


    }
}
