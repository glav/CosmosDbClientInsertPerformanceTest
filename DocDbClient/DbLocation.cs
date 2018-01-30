using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DocDbClient
{
    public class DbLocation
    {
        public DbLocation(string collectionId, string databaseId)
        {
            this.CollectionId = collectionId;
            this.DatabaseId = databaseId;
        }
        public string DatabaseId { get; set; }
        public string CollectionId { get; set; }
    }
}
