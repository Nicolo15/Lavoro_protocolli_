using NetCoreClient.ValueObjects;
using System.Text.Json;

namespace NetCoreClient.Sensors
{
    class VirtualWaterSensor : ISensorInterface
    {
        private readonly Random _random;

        // Nuove proprietà per id e cooler_id
        public string Id { get; private set; }
        public string CoolerId { get; private set; }

        public VirtualWaterSensor(string id, string coolerId)
        {
            _random = new Random();
            Id = id;
            CoolerId = coolerId;
        }

        // Simula i litri erogati (da 0 a 100)
        public int GetLitersDispensed()
        {
            return _random.Next(0, 101);
        }

        // Simula la temperatura dell'acqua (da 18 a 28 gradi Celsius)
        public int GetWaterTemperature()
        {
            return _random.Next(18, 29);
        }

        // Simula lo stato del filtro
        public string GetFilterStatus()
        {
            return _random.Next(0, 2) == 0 ? "Good" : "Needs replacement";
        }

        // Simula la luce notturna (accensione o spegnimento)
        public bool GetNightLightStatus()
        {
            return _random.Next(0, 2) == 0; // true o false
        }

        // Simula la modalità di manutenzione (on o off)
        public bool GetMaintenanceMode()
        {
            return _random.Next(0, 2) == 0; // true o false
        }

        // Metodo per serializzare i dati in JSON includendo id e cooler_id
        public string ToJson()
        {
            var data = new WaterData(
                GetLitersDispensed(),
                GetWaterTemperature(),
                GetFilterStatus(),
                GetNightLightStatus(),
                GetMaintenanceMode()
            )
            {
                Id = Id,                      // Aggiungi l'id
                CoolerId = CoolerId           // Aggiungi il cooler_id
            };

            return JsonSerializer.Serialize(data);
        }
    }
}
