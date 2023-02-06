using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System.Text;
using System.Transactions;

internal class Program
{
    private static async Task Main(string[] args)
    {
        string connectionString = "Endpoint=sb://vc0206sb.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=PwXMhPe8ZnznGckFmJ1qU+600MjF/SCcbqs5sgtHX2M=";
        string queueName = "session-demo-queue";

        await CreateQueue(connectionString, queueName);
        await SessionProcessor(connectionString, queueName);
        await SessionQueue(connectionString, queueName);

        // since ServiceBusClient implements IAsyncDisposable we create it with "await using"
        await TransactionDemo(connectionString, "queueA");
        await CrossEntityTransaction(connectionString);


        static async Task SessionProcessor(string connectionString, string queueName)
        {
            var client = new ServiceBusClient(connectionString);

            var options = new ServiceBusSessionProcessorOptions
            {
                // By default or when AutoCompleteMessages is set to true, the processor will complete the message after executing the message handler
                // Set AutoCompleteMessages to false to [settle messages](https://docs.microsoft.com/en-us/azure/service-bus-messaging/message-transfers-locks-settlement#peeklock) on your own.
                // In both cases, if the message handler throws an exception without settling the message, the processor will abandon the message.
                AutoCompleteMessages = false,
                // I can also allow for multi-threading
                SessionIds = { "mySessionId2", "mySessionId1" }

            };


            ServiceBusSessionProcessor processor = client.CreateSessionProcessor(queueName, options);

            // configure the message and error handler to use
            processor.ProcessMessageAsync += MessageHandler;
            processor.ProcessErrorAsync += ErrorHandler;

            async Task MessageHandler(ProcessSessionMessageEventArgs args)
            {
                string body = args.Message.Body.ToString();
                Console.WriteLine(body);

                // we can evaluate application logic and use that to determine how to settle the message.
                await args.CompleteMessageAsync(args.Message);
            }

            Task ErrorHandler(ProcessErrorEventArgs args)
            {
                // the error source tells me at what point in the processing an error occurred
                Console.WriteLine(args.ErrorSource);
                // the fully qualified namespace is available
                Console.WriteLine(args.FullyQualifiedNamespace);
                // as well as the entity path
                Console.WriteLine(args.EntityPath);
                Console.WriteLine(args.Exception.ToString());
                return Task.CompletedTask;
            }

            // start processing
            await processor.StartProcessingAsync();

        }

        static async Task CreateQueue(string connectionString, string queueName)
        {
            var client = new ServiceBusAdministrationClient(connectionString);
            if (!client.QueueExistsAsync(queueName).Result)
            {
                var options = new CreateQueueOptions(queueName)
                {
                    AutoDeleteOnIdle = TimeSpan.FromDays(7),
                    DefaultMessageTimeToLive = TimeSpan.FromDays(2),
                    DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(1),
                    EnableBatchedOperations = true,
                    DeadLetteringOnMessageExpiration = true,
                    EnablePartitioning = false,
                    ForwardDeadLetteredMessagesTo = null,
                    ForwardTo = null,
                    LockDuration = TimeSpan.FromSeconds(45),
                    MaxDeliveryCount = 8,
                    MaxSizeInMegabytes = 2048,
                    RequiresDuplicateDetection = true,
                    RequiresSession = true,
                    UserMetadata = "some metadata"
                };

                options.AuthorizationRules.Add(new SharedAccessAuthorizationRule(
                    "allClaims",
                    new[] { AccessRights.Manage, AccessRights.Send, AccessRights.Listen }));

                QueueProperties createdQueue = await client.CreateQueueAsync(options);
            }
        }

        static async Task SessionQueue(string connectionString, string queueName)
        {
            // since ServiceBusClient implements IAsyncDisposable we create it with "await using"
            var client = new ServiceBusClient(connectionString);

            // create the sender
            ServiceBusSender sender = client.CreateSender(queueName);

            // create a session message that we can send
            await SendSessionMessages(sender);

            // create a session receiver that we can use to receive the message. Since we don't specify a
            // particular session, we will get the next available session from the service.
            ServiceBusSessionReceiver receiver = await client.AcceptSessionAsync(queueName, "mySessionId0");

            // the received message is a different type as it contains some service set properties

            ServiceBusReceivedMessage receivedMessage = await receiver.ReceiveMessageAsync();
            Console.WriteLine(receivedMessage.SessionId);

            // we can also set arbitrary session state using this receiver
            // the state is specific to the session, and not any particular message
            await receiver.SetSessionStateAsync(new BinaryData("some state"));

            // the state can be retrieved for the session as well
            BinaryData state = await receiver.GetSessionStateAsync();

        }

        static async Task SendSessionMessages(ServiceBusSender sender)
        {
            for (int i = 0; i < 10; i++)
            {
                ServiceBusMessage message = new ServiceBusMessage(Encoding.UTF8.GetBytes($"Hello world! mySessionId{i % 3}"))
                {
                    SessionId = $"mySessionId{i % 3}"
                };

                // send the message
                await sender.SendMessageAsync(message);
            }

        }
    }

    static async Task CrossEntityTransaction(string connectionString)
    {
        var options = new ServiceBusClientOptions { EnableCrossEntityTransactions = true };
        await using var client = new ServiceBusClient(connectionString, options);

        ServiceBusReceiver receiverA = client.CreateReceiver("queueA");
        ServiceBusSender senderB = client.CreateSender("queueB");
        ServiceBusSender senderC = client.CreateSender("topicC");

        ServiceBusReceivedMessage receivedMessage = await receiverA.ReceiveMessageAsync();

        using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await receiverA.CompleteMessageAsync(receivedMessage);
            await senderB.SendMessageAsync(new ServiceBusMessage());
            await senderC.SendMessageAsync(new ServiceBusMessage());
            ts.Complete();
        }
    }

    static async Task TransactionDemo(string connectionString,string queueName)
    {
        var client = new ServiceBusClient(connectionString);
        ServiceBusSender sender = client.CreateSender(queueName);

        await sender.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes("First")));
        ServiceBusReceiver receiver = client.CreateReceiver(queueName);
        ServiceBusReceivedMessage firstMessage = await receiver.ReceiveMessageAsync();
        using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes("Second")));
            await receiver.CompleteMessageAsync(firstMessage);
            ts.Complete();
        }

    }
}