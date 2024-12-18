using NetCoreClient.Sensors;
using NetCoreClient.Protocols;

// define sensors
List<ISensorInterface> sensors = new();
sensors.Add(new VirtualWaterTempSensor());
sensors.Add(new VirtualLightSensor());

// define protocol
// ProtocolInterface protocol = new Http("http://localhost:8011/casetta/v1/+/sensori");
IProtocolInterface protocol = new Amqp("amqps://nnpejxpu:wVyTzPikUKw63EzRruNgwqXD6uKlHbbr@cow.rmq2.cloudamqp.com/nnpejxpu");

// send data to server
while (true)
{
    foreach (ISensorInterface sensor in sensors)
    {
        var sensorValue = sensor.ToJson();

        protocol.Send(sensorValue, sensor.GetSlug());

        Console.WriteLine("Data sent: " + sensorValue);

        Thread.Sleep(1000);
    }

}