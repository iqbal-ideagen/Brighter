#region Licence
/* The MIT License (MIT)
Copyright © 2017 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using System;
using Confluent.Kafka;
using Greetings.Ports.Commands;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.Kafka;
using Serilog;
using Serilog.Extensions.Logging;

namespace GreetingsSender
{
    class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            var serviceCollection = new ServiceCollection();
            serviceCollection.AddSingleton<ILoggerFactory>(new SerilogLoggerFactory());

            var kafkaConfiguration = new KafkaMessagingGatewayConfiguration
            {
                Name = "icsa.event.bus",
                BootStrapServers = new[] { "localhost:9093" },
                SaslUsername = String.Empty,
                SaslPassword = String.Empty,
                SaslMechanisms = null,
                SecurityProtocol = null
            };

            var consumerFactory = new KafkaMessageConsumerFactory(kafkaConfiguration);

            var replySubscriptions = new[]
            {
                new KafkaSubscription<GreetingReply>(
                new SubscriptionName("create.task.reply.subscription"),
                channelName: new ChannelName("create.task.reply.channel"),
                // routingKey: new RoutingKey("Reply"),
                groupId: "iqbal.reply",
                timeoutInMilliseconds: 100,
                commitBatchSize:5)
            };

            serviceCollection
                .AddBrighter(options =>
                {
                    options.ChannelFactory = new ChannelFactory(consumerFactory);
                })
                .UseInMemoryOutbox()
                .UseExternalBus(
                    new KafkaProducerRegistryFactory(
                        kafkaConfiguration,
                        new[]
                        {
                            new KafkaPublication
                            {
                                Topic = new RoutingKey("Greeting.Request"),
                                MessageSendMaxRetries = 3,
                                MessageTimeoutMs = 1000,
                                MaxInFlightRequestsPerConnection = 1
                            }
                        }).Create(),
                    true,
                    replySubscriptions)
                .AutoFromAssemblies();

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var commandProcessor = serviceProvider.GetService<IAmACommandProcessor>();

            Console.WriteLine("Requesting Salutation...");

            //blocking call
            commandProcessor.Call<GreetingRequest, GreetingReply>(new GreetingRequest { Name = "Ian", Language = "en-gb" }, 2000);

            Console.WriteLine("Done...");
            Console.ReadLine();
        }
    }
}
