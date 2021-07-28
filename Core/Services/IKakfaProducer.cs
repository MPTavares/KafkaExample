using Core.DTO;
using Core.Options;
using System.Threading.Tasks;

namespace Core.Services
{
    public interface IKakfaProducer<T>
    {
        Task<ResponseDTO> SendMessage(string topic, T payload);
    }
}