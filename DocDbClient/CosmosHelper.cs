using Microsoft.Azure.Documents;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace DocDbClient
{
    public static class CosmosHelper
    {
        public static async Task DoOperationWithRetryAsync(Func<Task> action, int maximumRetryAmount)
        {
            bool taskComplete = false;
            int requestAttempts = 0;
            while (!taskComplete)
            {
                try
                {
                    requestAttempts++;
                    if (requestAttempts > maximumRetryAmount)
                    {
                        taskComplete = true;  // will cause exist from the loop but will allow just one more try :-)
                    }
                    await action();
                    taskComplete = true;
                }
                catch (DocumentClientException dce)
                {
                    if (ShouldRetryOperation(dce.StatusCode, requestAttempts, maximumRetryAmount))
                    {
                        Logger.Error($"Request rate throttled (attempt #{requestAttempts}), retrying");
                        await Task.Delay(dce.RetryAfter);
                    }
                    throw;
                }
                catch (AggregateException ae)
                {
                    var docExcep = ae.InnerException as DocumentClientException;
                    if (docExcep != null && ShouldRetryOperation(docExcep.StatusCode, requestAttempts, maximumRetryAmount))
                    {
                        Logger.Error($"Request rate throttled (attempt #{requestAttempts}), retrying");
                        await Task.Delay(docExcep.RetryAfter);
                    }
                    else
                    {
                        Logger.Error($"Other error encountered: {ae.Message}");
                        throw;
                    }
                }

            }
        }

        private static bool ShouldRetryOperation(HttpStatusCode? statusCode, int requestAttempts, int maximumRetryAmount)
        {
            if (requestAttempts >= maximumRetryAmount)
            {
                return false;
            }
            var statusCodeValue = statusCode.HasValue ? (int)statusCode : 429; // If null passed as status, assume a failure code
            if (statusCodeValue == 429 || statusCodeValue == 503)
            {
                return true;
            }
            return false;

        }


    }
}
