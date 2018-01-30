namespace DocDbClient
{
    using System;
    using System.Net;
    using System.Threading.Tasks;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;


    public class CosmosWriteOperations<T> where T : class
    {
        private readonly DbLocation dataLocation;
        private readonly CosmosDbConfiguration cosmosDbConfiguration;
        private readonly DocumentClient documentClient;

        public CosmosWriteOperations(DbLocation dataLocation,
            CosmosDbConfiguration cosmosDbConfiguration)
        {
            this.dataLocation = dataLocation;
            this.cosmosDbConfiguration = cosmosDbConfiguration;
            this.documentClient = new DocumentClient(cosmosDbConfiguration.Endpoint, cosmosDbConfiguration.Key, new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp, MaxConnectionLimit=1000 });
            this.documentClient.OpenAsync();  // initialise the client, and ensure all routes are pre-fetched.
        }

        public async Task CreateDocumentInCollection(Uri uri,T document)
        {
            await this.documentClient.UpsertDocumentAsync(uri, document);
        }

        public async Task CreateDocumentCollectionIfNotExists()
        {
            await this.documentClient.CreateDatabaseIfNotExistsAsync(new Database { Id = this.dataLocation.DatabaseId });
            var hostedInCloud = !String.IsNullOrEmpty(Environment.GetEnvironmentVariable("WEBSITE_SITE_NAME"));
            // Note: We test for hosting in cloud so that if being run locally, no partition key and offer throughput
            //       is used in collection creation. While this works locally, the CosmosDb emulator Data explorer does not
            //       display any documents if using a partition key and actually errors making it look like there is no data
            //       when in fact there is.
            if (hostedInCloud)
            {
                await this.documentClient.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(this.dataLocation.DatabaseId),
                    new DocumentCollection { Id = this.dataLocation.CollectionId, PartitionKey = PartitionScheme.PartitionKeyPaths },
                    new RequestOptions { OfferThroughput = this.cosmosDbConfiguration.Throughput, ConsistencyLevel = ConsistencyLevel.Eventual  });
            }
            else
            {
                await this.documentClient.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(dataLocation.DatabaseId),
                    new DocumentCollection { Id = dataLocation.CollectionId });
            }

        }
    }
}