
using SparkplugNet.Core.Application;
using SparkplugNet.Core.Enumerations;
using SparkplugNet.VersionB;
using SparkplugNet.VersionB.Data;


namespace SparkplugCommandSender
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Load or hardcode your config (matching the device's config.json)
            string brokerAddress = "localhost";
            int brokerPort = 1883;
            string groupId = "Automotive";
            string nodeId = "FactoryFloor";
            string deviceId = "SolenoidValve1";
            string scadaHostId = "SCADA_Primary";  // Matches the device's scadaHostIdentifier

            // Prepare the DCMD metrics (set "Device Control/Repair" to true)
            var commandMetrics = new List<Metric>
            {
                new(name: "Device Control/Repair", DataType.Boolean, true)
            };

            // Create SparkplugApplication (acts as primary host)
            var application = new SparkplugApplication(commandMetrics, SparkplugSpecificationVersion.Version30);

            // Start the application (connect to broker)
            await application.Start(new SparkplugApplicationOptions(
                brokerAddress: brokerAddress,
                port: brokerPort,
                clientId: $"{scadaHostId}_Commander",
                scadaHostIdentifier: scadaHostId,
                reconnectInterval: TimeSpan.FromSeconds(30),
                mqttProtocolVersion: SparkplugMqttProtocolVersion.V500  // Explicitly use MQTT 5.0
            ));

            Console.WriteLine("Connected to broker as primary host.");

            

            // Publish the DCMD to the specific device
            await application.PublishDeviceCommand(commandMetrics, groupId, nodeId, deviceId);

            Console.WriteLine("Repair command sent. The device should reset its health score to 1.0.");

            // Optional: Keep running or handle STATE messages if needed
            // For now, wait and then stop
            await Task.Delay(5000);  // Give time for delivery
            await application.Stop();
        }
    }
}