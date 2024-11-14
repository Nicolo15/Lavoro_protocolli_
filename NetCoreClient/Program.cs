using NetCoreClient.Sensors;
using NetCoreClient.Protocols;
using System;
using System.Collections.Generic;
using System.Threading;

class Program
{
    static void Main(string[] args)
    {
        // Definisce i sensori con id e cooler_id
        List<ISensorInterface> sensors = new();

        // Creazione di un sensore virtuale con id e cooler_id
        sensors.Add(new VirtualWaterSensor("12345", "cooler123")); // Passa id e cooler_id

        // Definisce il protocollo di comunicazione HTTP
        ProtocolInterface protocol = new Http("http://localhost:8011/water_coolers/69" +
            ""); // Endpoint del server

        // Ciclo per inviare i dati al server
        while (true)
        {
            foreach (ISensorInterface sensor in sensors)
            {
                // Converti i dati del sensore in formato JSON
                var sensorValue = sensor.ToJson();

                // Invia i dati al server
                protocol.Send(sensorValue);

                // Mostra il JSON inviato
                Console.WriteLine("Dati inviati: " + sensorValue);

                // Pausa di 1 secondo
                Thread.Sleep(1000);
            }
        }
    }
}
