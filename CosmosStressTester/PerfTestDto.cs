using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosStressTester
{
    public class PerfTestDto
    {
        public int KeyNumber { get; set; }
        public string SomeText { get; set; }
        public int SomeCount { get; set; }

        public string PartitionKey => Guid.NewGuid().ToString();
    }
}
