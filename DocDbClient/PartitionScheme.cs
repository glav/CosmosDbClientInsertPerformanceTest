using Microsoft.Azure.Documents;

namespace DocDbClient
{
    public static class PartitionScheme
    {

        static PartitionScheme()
        {
            PartitionKeyPaths = new PartitionKeyDefinition();
            PartitionKeyPaths.Paths.Add("/PartitionKey");

        }

        public static PartitionKeyDefinition PartitionKeyPaths { get; }
    }
}
