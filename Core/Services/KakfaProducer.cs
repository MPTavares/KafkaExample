using Confluent.Kafka;
using Core.DTO;
using Core.Options;
using Core.Serializers;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Core.Services
{
    public class KakfaProducer<T> : IKakfaProducer<T>
    {
        private readonly IOptions<ProducerKafkaOptions> _producerConfig;

        public KakfaProducer(IOptions<ProducerKafkaOptions> producerConfig)
        {
            _producerConfig = producerConfig;
        }
        public async Task<ResponseDTO> SendMessage(string topic, T payload)
        {
            var config = new ProducerConfig(new ProducerConfig
            {
                BootstrapServers = _producerConfig.Value.BootstrapServers,
                RequestTimeoutMs = 2000,
                MessageTimeoutMs = 2000
            });

            var sendMessage = new Message<Guid, string>
            {
                Key = Guid.NewGuid(),
                Value = JsonSerializer.Serialize(payload)
            };

            using (var producer = new ProducerBuilder<Guid, string>(config).SetKeySerializer(new CustomSerializer<Guid>()).SetValueSerializer(new CustomSerializer<string>()).Build())
            {
                try
                {
                    var sendProducer = producer
                        .ProduceAsync(topic, sendMessage)
                        .GetAwaiter()
                        .GetResult();

                    return await Task.FromResult(new ResponseDTO
                    {
                        Status = true,
                        Message = $"Mensagem: [{sendProducer.Value}, offSet:{sendProducer.TopicPartitionOffset}"
                    });
                }
                catch (ProduceException<Null, string> e)
                {
                    return await Task.FromResult(new ResponseDTO
                    {
                        Status = false,
                        Message = $"Erro ao enviar mensagem: {e.Error.Reason}"
                    });
                }
            }
        }
    }
}
