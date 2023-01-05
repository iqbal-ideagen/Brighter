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
using System.Threading.Tasks;
using Confluent.Kafka;
using Greetings.Ports.Commands;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.Kafka;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;
using Serilog;
using ChannelFactory = Paramore.Brighter.MessagingGateway.Kafka.ChannelFactory;

namespace GreetingsServer
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            var host = new HostBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    var subscriptions = new KafkaSubscription[]
                    {
                        new KafkaSubscription<GreetingRequest>(
                            new SubscriptionName("paramore.example.greeting"),
                            new ChannelName("Greeting.Request"),
                            new RoutingKey("Greeting.Request"),
                            groupId: "test.request",
                            timeoutInMilliseconds: 500,
                            offsetDefault: AutoOffsetReset.Earliest,
                            commitBatchSize:350,
                            isAsync: false,
                            noOfPerformers: 1)
                    };

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

                    services.AddServiceActivator(options =>
                    {
                        options.Subscriptions = subscriptions;
                        options.ChannelFactory = new ChannelFactory(consumerFactory);
                    })
                    .UseInMemoryOutbox()
                    .UseExternalBus(
                        new KafkaProducerRegistryFactory(
                            kafkaConfiguration,
                            new KafkaPublication[]
                            {
                                new()
                                {
                                    Topic = new RoutingKey("Reply"), //topic
                                    MessageSendMaxRetries = 3,
                                    MessageTimeoutMs = 1000,
                                    MaxInFlightRequestsPerConnection = 1
                                }
                            }).Create(),
                        true)
                    .AutoFromAssemblies();


                    services.AddHostedService<ServiceActivatorHostedService>();
                })
                .UseConsoleLifetime()
                .UseSerilog()
                .Build();

            await host.RunAsync();
        }
    }
}
