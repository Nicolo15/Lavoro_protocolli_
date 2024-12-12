using NetCoreClient.ValueObjects;
using System.Text.Json;

namespace NetCoreClient.Sensors
{
    class VirtualLightSensor : ISensorInterface
    {
        private readonly Random Random;

        public VirtualLightSensor()
        {
            Random = new Random();
        }

        /// <summary>
        /// Simula lo stato della luce: 1 per acceso, 0 per spento.
        /// </summary>
        public int LightState()
        {
            return Random.Next(2); // Genera 0 o 1 casualmente
        }

        /// <summary>
        /// Converte lo stato in formato JSON.
        /// </summary>
        public string ToJson()
        {
            return JsonSerializer.Serialize(LightState());
        }

        /// <summary>
        /// Restituisce lo slug identificativo del sensore.
        /// </summary>
        public string GetSlug()
        {
            return "/lightstate";
        }
    }
}
