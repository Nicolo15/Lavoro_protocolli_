using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NetCoreClient.Protocols
{
    interface ProtocolInterface
    {
        void Send(string data);
    }
}
