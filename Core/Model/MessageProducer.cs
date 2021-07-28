using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Model
{
    public class MessageProducer<T>
    {
        public MessageProducer(Guid id, T payload)
        {
            Id = id;
            this.payload = payload;
        }

        public Guid Id { get; set; }
        public T payload { get; set; }

    }
}
