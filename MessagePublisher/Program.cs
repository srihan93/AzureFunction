using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using System.Collections.Generic;

namespace MessagePublisher
{
   public class Program
    {
        public static string connectionString = "Endpoint=sb://practisedevenv.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Jj/6EAQFnVFvYgRQPLV2smF59WZPzBP69uTPMaydKpg=";

        public static string topicName = "PrasanthDemo";

        public static string subscriptionName = "PrasanthSubscription";

        public static ManagementClient managementClient;

        public static ServiceBusClient client;

        public static ServiceBusSender sender;

        public static async Task Main()
        {
            managementClient = new ManagementClient(connectionString);
            var topicExists = await managementClient.TopicExistsAsync(topicName);
            if (!topicExists)
            {
                var topicDescription = await managementClient.CreateTopicAsync(new TopicDescription(topicName) { EnablePartitioning = true });

                if (!string.IsNullOrWhiteSpace(topicDescription.Path) && topicDescription.Status==EntityStatus.Active)
                {
                    SubscriptionDescription subscriptionDescription = new SubscriptionDescription(topicName, subscriptionName)
                    {
                        MaxDeliveryCount = 3,
                        LockDuration = TimeSpan.FromSeconds(60),
                        DefaultMessageTimeToLive = TimeSpan.FromDays(2),
                        AutoDeleteOnIdle = TimeSpan.FromDays(2),
                        TopicPath = topicName,
                        SubscriptionName = subscriptionName
                    };

                    RuleDescription subscriptionRule = new RuleDescription { Name = "Create", Filter= new SqlFilter("Action='c'") };

                    //TODO : Subscription Rule to be added.
                    await managementClient.CreateSubscriptionAsync(subscriptionDescription);
                }
            }
            
            client = new ServiceBusClient(connectionString);
            sender = client.CreateSender(topicName);            

            var additionalProperties = new Dictionary<string, object>
            {
                { "StudentId", 1 },
                { "Email Id", "prasanth@mail.com" }
            };

            var eventModel = new EventModel { Action = "c", ActionContext = "c_students", Entity = topicName, CorrelationId = Guid.NewGuid().ToString(), AdditionalProperties = additionalProperties };

            string jsonEventModel = Newtonsoft.Json.JsonConvert.SerializeObject(eventModel);

            var messages = new List<ServiceBusMessage> { new ServiceBusMessage(jsonEventModel)
            {
                CorrelationId = eventModel.CorrelationId,
                MessageId = Guid.NewGuid().ToString(),
                ContentType = "application/json",
            } };

            try
            {
                await sender.SendMessagesAsync(messages);              
            }
            catch (Exception ex)
            {
                string messgage = ex.Message;
            }
            finally
            {                
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }
        }
    }

    public class EventModel
    {
        public string CorrelationId { get; set; }

        public string Entity { get; set; }

        public string Action { get; set; }

        public string ActionContext { get; set; }

        public Dictionary<string,object> AdditionalProperties { get; set; }
    }
}
