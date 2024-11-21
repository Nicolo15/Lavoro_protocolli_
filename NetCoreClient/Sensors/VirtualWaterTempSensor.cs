using NetCoreClient.ValueObjects;
using System.Text.Json;

namespace NetCoreClient.Sensors
{
    class VirtualWaterTempSensor : IWaterTempSensorInterface, ISensorInterface
    {
        private readonly Random Random;

        public VirtualWaterTempSensor()
        {
            Random = new Random();
        }

        public int WaterTemperature()
        {
            return new WaterTemperature(Random.Next(20)).Value;
        }

        public string ToJson()
        {
            return JsonSerializer.Serialize(WaterTemperature());
        }

        public string GetSlug()
        {
            return "/watertemp";
        }
    }
}