using Confluent.Kafka;
using Core;
using Core.DTO;
using Core.Model;
using Core.Options;
using Core.Serializers;
using Core.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace APIs.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class UserController : ControllerBase
    {
        private readonly ProducerKafkaOptions _producerConfig;
        private readonly IKakfaProducer<UserPayload> _kafkaProducer;
        public UserController(IOptions<ProducerKafkaOptions> producerConfig, IKakfaProducer<UserPayload> kafkaProducer)
        {
            _producerConfig = producerConfig.Value;
            _kafkaProducer = kafkaProducer;
        }
        /// <summary>
        /// Create a user sending the data to topic kafka "CREATE_USER".
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>

        [HttpPost ("/User")]
        public async Task<ResponseDTO> SendMessageUserCreated(UserPayload user)
        {

            var responseProducer = await _kafkaProducer.SendMessage("USER_CREATE", user);
            return await Task.FromResult(responseProducer);

        }
        /// <summary>
        /// Send message to topic kafka "WELCOME_EMAIL".
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        [HttpPost("/Email")]
        public async Task<ResponseDTO> SendMessageWelcomeEmail(UserPayload user)
        {
            var responseProducer = await _kafkaProducer.SendMessage("WELCOME_EMAIL", user);
            return await Task.FromResult(responseProducer);
        }
       
        [HttpGet ("/")]
        public async Task<string> Welcome()
        {
            return await Task.FromResult("Welcome");
        }
    }
}
