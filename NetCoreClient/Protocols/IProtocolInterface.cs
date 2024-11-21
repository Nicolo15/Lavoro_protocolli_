using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NetCoreClient.Protocols
{
    interface IProtocolInterface
    {
        void Send(string data, string sensor);
    }
}