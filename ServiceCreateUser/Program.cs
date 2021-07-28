using Confluent.Kafka;
using Core;
using Core.Deserializers;
using Core.Options;
using Core.Services;
using Microsoft.Extensions.Options;
using System;
using System.Diagnostics;
using System.Text.Json;
using System.Threading;

namespace ServiceCreateUser
{
    class Program
    {
        static void Main(string[] args)
        {

            Console.WriteLine($"Started consumer - ServiceCreateUser, Ctrl-C to stop consuming");
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            Consumer("USER_CREATE", cts.Token);

        }

        private static async void Consumer(string topic, CancellationToken token)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "user-create-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                AllowAutoCreateTopics = true
            };

            IOptions<ProducerKafkaOptions> configProducer = Options.Create(new ProducerKafkaOptions());
            configProducer.Value.BootstrapServers = config.BootstrapServers;
            var producer = new KakfaProducer<UserPayload>(configProducer);

            using (var consumer = new ConsumerBuilder<Guid, string>(config).SetKeyDeserializer(new CustomDeserializer<Guid>()).SetValueDeserializer(new CustomDeserializer<string>()).Build())
            {
                consumer.Subscribe(topic);               
                try
                {
                    while (true)
                    {
                        var message = consumer.Consume(token);

                        UserPayload user = JsonSerializer.Deserialize<UserPayload>(message.Message.Value);

                        Console.WriteLine($"-------------------- CREATE USER  ---------------------------------");
                        Console.WriteLine($" Message: {message.Message.Value}");
                        Console.WriteLine($" Name: {user.Name}");
                        Console.WriteLine($" Email: {user.Email}");
                        Console.WriteLine($" Topic: {message.Topic}");
                        Console.WriteLine($" Offset: {message.Offset}");
                        Console.WriteLine($" Partition: {message.Partition}");

                        //Save user in database. If user do not exists send welcome email for example.
                        //For testing purpose, i send message to another topic

                        var response = await producer.SendMessage("WELCOME_EMAIL", user);
                        Console.WriteLine($" Send WELCOME_EMAIL: {response.Status}");
                        Console.WriteLine($" message: {message.Message}");
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
