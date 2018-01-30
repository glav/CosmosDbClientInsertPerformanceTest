using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DocDbClient
{
    public class CosmosDbConfiguration
    {
        private CosmosDbConfiguration()
        {
        }

        public Uri Endpoint { get; private set; }

        public string Key { get; private set; }

        public int Throughput { get; private set; }

        public static CosmosDbConfiguration CreateValidConfiguration(
            string endpoint,
            string key,
            string throughputConfigValue)
        {
            var config = new CosmosDbConfiguration { Endpoint = new Uri(endpoint) };
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException("key", "CosmosDb requires an access key");
            }
            config.Key = key;

            int throughputActualValue;
            if (int.TryParse(throughputConfigValue, out throughputActualValue))
            {
                config.Throughput = throughputActualValue < 400 ? 400 : throughputActualValue;
            }
            else
            {
                config.Throughput = 400;
            }
            return config;
        }
    }
}
