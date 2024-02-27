using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace AzureFunction_EventHub_Demo
{
	public class TestData
	{
		public string Country { get; set; }
		public string City { get; set; }
		
	}
	public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run([EventHubTrigger("sampleevents", Connection = "eventhubconnection")] EventData[] events,
            [Sql("[dbo].[TestData]", "sqlconnection")] IAsyncCollector<TestData> output,
			ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    // Replace these two lines with your processing logic.
                    log.LogInformation($"C# Event Hub trigger function processed a message: {eventData.EventBody}");
					//string requestBody = await new StreamReader(eventData.EventBody).ReadToEndAsync();
					string requestbody = eventData.EventBody.ToString();
					TestData todoitem = JsonConvert.DeserializeObject<TestData>(requestbody);
					await output.AddAsync(todoitem);
					await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
