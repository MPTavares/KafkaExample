using Confluent.Kafka;
using Core;
using Core.Deserializers;
using Core.Options;
using Core.Services;
using Microsoft.Extensions.Options;
using System;
using System.Text.Json;
using System.Threading;

namespace ServiceWelcomeEmail
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Started consumer - ServiceWelcomeEmail, Ctrl-C to stop consuming");

            var cts = new CancellationTokenSource();

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            Consumer("WELCOME_EMAIL", cts.Token);
        }

        private static void Consumer(string topic, CancellationToken token)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "user-create-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                AllowAutoCreateTopics = true,
                
            };

            IOptions<ProducerKafkaOptions> configProducer = Options.Create(new ProducerKafkaOptions());
            configProducer.Value.BootstrapServers = config.BootstrapServers;
            var producer = new KakfaProducer<UserPayload>(configProducer);

            using (var consumer = new ConsumerBuilder<Guid, string>(config).SetKeyDeserializer(new Core.Deserializers.CustomDeserializer<Guid>()).SetValueDeserializer(new CustomDeserializer<string>()).Build())
            {
                consumer.Subscribe(topic);
                try
                {
                    while (true)
                    {
                        var message = consumer.Consume(token);

                        UserPayload user = JsonSerializer.Deserialize<UserPayload>(message.Message.Value);

                        Console.WriteLine($"-------------------- WELCOME EMAIL ---------------------------------");
                        Console.WriteLine($" Message: {message.Message.Value}");
                        Console.WriteLine($" Name: {user.Name}");
                        Console.WriteLine($" Email: {user.Email}");
                        Console.WriteLine($" Topic: {message.Topic}");
                        Console.WriteLine($" Offset: {message.Offset}");
                        Console.WriteLine($" Partition: {message.Partition}");                                           

                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }
    }
}
