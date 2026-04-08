using Microsoft.Data.Sqlite;
using Polly;
using Serilog;
using SparkplugNet.Core;
using SparkplugNet.Core.Enumerations;
using SparkplugNet.Core.Node;
using SparkplugNet.VersionB;
using SparkplugNet.VersionB.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Industrial.Solenoid.Ultra
{
    #region Models
    public class Config
    {
        public string BrokerAddress { get; set; } = "localhost";
        public int BrokerPort { get; set; } = 1883;
        public string GroupId { get; set; } = "Automotive";
        public string NodeId { get; set; } = "FactoryFloor";
        public string DeviceId { get; set; } = "SolenoidValve1";
        public int ScanRateMs { get; set; } = 2000;
        public string SqliteDbPath { get; set; } = "cache.db";
        public double TempBuildupFactor { get; set; } = 0.0001;
        public double WearFactor { get; set; } = 0.00001;
        public double NoiseAmplitude { get; set; } = 0.05;
        public Dictionary<string, bool> MetricsEnabled { get; set; } = new();
    }

    public class SolenoidTwin
    {
        private const double BaseResistance = 12.0;
        private const double ThermalCoeff = 0.00393;
        private const double AmbientTemp = 25.0;
        public double InternalTemp { get; private set; } = 25.0;
        public long TotalCycles { get; set; } = 0;
        public double HealthScore { get; private set; } = 1.0;

        public void ProcessCycle(Config config, Random rng)
        {
            TotalCycles++;
            double heatGenerated = 0.5 + (config.TempBuildupFactor * 100);
            InternalTemp += heatGenerated - (0.1 * (InternalTemp - AmbientTemp));
            HealthScore = Math.Max(0, HealthScore - config.WearFactor);
        }

        public void Repair() => HealthScore = 1.0;
        public void ResetStats() => TotalCycles = 0;

        public Dictionary<string, object> GetTelemetry(Config config, Random rng)
        {
            double noise = (rng.NextDouble() * 2 - 1) * config.NoiseAmplitude;
            double resistance = BaseResistance * (1 + ThermalCoeff * (InternalTemp - 20.0));
            double voltage = 24.0 + noise;

            return new Dictionary<string, object>
            {
                { "Status", HealthScore < 0.2 ? "Critical" : HealthScore < 0.6 ? "Warning" : "Healthy" },
                { "ProductionCount", TotalCycles },
                { "TemperatureC", InternalTemp + noise },
                { "CoilResistanceOhm", resistance },
                { "PullInVoltageV", voltage },
                { "PullInCurrentA", voltage / resistance },
                { "HydraulicPressurePsi", 1200.0 * HealthScore + (noise * 20) },
                { "AirPressurePsi", 90.0 + noise },
                { "AirFlowRateLpm", 15.0 * HealthScore + noise },
                { "DefectRate", (1.0 - HealthScore) * 0.1 },
                { "LeakRateCcMin", (1.0 - HealthScore) * 50.0 + Math.Max(0, noise) },
                { "VibrationPeakG", (1.0 - HealthScore) * 5.0 + 0.2 + Math.Abs(noise) },
                { "HealthScore", HealthScore * 100.0 }
            };
        }
    }
    #endregion

    #region SQLite Buffer
    public class SqliteBuffer
    {
        private readonly string _connStr;
        public SqliteBuffer(string path) { _connStr = $"Data Source={path}"; Init(); }
        private void Init()
        {
            using var conn = new SqliteConnection(_connStr); conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE IF NOT EXISTS Telemetry (Id INTEGER PRIMARY KEY, Payload TEXT)";
            cmd.ExecuteNonQuery();
        }
        public void Enqueue(List<Metric> metrics)
        {
            using var conn = new SqliteConnection(_connStr); conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO Telemetry (Payload) VALUES (@p)";
            cmd.Parameters.AddWithValue("@p", JsonSerializer.Serialize(metrics.Select(m => new { m.Name, m.Value, m.DataType })));
            cmd.ExecuteNonQuery();
        }
        public List<(int Id, List<Metric> Metrics)> Fetch(int limit)
        {
            var res = new List<(int, List<Metric>)>();
            using var conn = new SqliteConnection(_connStr); conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT Id, Payload FROM Telemetry LIMIT @l";
            cmd.Parameters.AddWithValue("@l", limit);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var raw = JsonSerializer.Deserialize<List<MetricDto>>(reader.GetString(1)) ?? new();
                var metrics = raw.Select(r => {
                    var m = new Metric { Name = r.Name };
                    object val = r.Value is JsonElement e ? ConvertJson(e, r.DataType) : r.Value;
                    m.SetValue(r.DataType, val);
                    return m;
                }).ToList();
                res.Add((reader.GetInt32(0), metrics));
            }
            return res;
        }
        private object ConvertJson(JsonElement e, DataType t) => t switch
        {
            DataType.Double => e.GetDouble(),
            DataType.Int64 => e.GetInt64(),
            DataType.Boolean => e.GetBoolean(),
            _ => e.GetString() ?? ""
        };
        public void Clear(List<int> ids)
        {
            if (!ids.Any()) return;
            using var conn = new SqliteConnection(_connStr); conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DELETE FROM Telemetry WHERE Id IN ({string.Join(",", ids)})";
            cmd.ExecuteNonQuery();
        }
        private class MetricDto { public string Name { get; set; } = ""; public object Value { get; set; } = 0; public DataType DataType { get; set; } }
    }
    #endregion

    class Program
    {
        private static readonly JsonSerializerOptions _jsonOptions = new() { PropertyNameCaseInsensitive = true, ReadCommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true };
        static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration().MinimumLevel.Debug().WriteTo.Console().CreateLogger();

            Config config = JsonSerializer.Deserialize<Config>(File.ReadAllText("config.json"), _jsonOptions)!;
            var buffer = new SqliteBuffer(config.SqliteDbPath);
            var twin = new SolenoidTwin();
            var rng = new Random();
            var node = new SparkplugNode(new List<Metric>(), SparkplugSpecificationVersion.Version30);
            bool isOnline = false;

            // Connection Task
            _ = Task.Run(async () => {
                while (true)
                {
                    if (!isOnline)
                    {
                        try
                        {
                            await node.Start(new SparkplugNodeOptions(
                                brokerAddress: config.BrokerAddress,
                                port: config.BrokerPort,
                                clientId: $"{config.NodeId}_Sim",
                                scadaHostIdentifier: "SCADA_Primary",
                                reconnectInterval: TimeSpan.FromSeconds(30),
                                mqttProtocolVersion: SparkplugMqttProtocolVersion.V500,
                                groupIdentifier: config.GroupId,
                                edgeNodeIdentifier: config.NodeId
                                ));
                            await node.PublishDeviceBirthMessage(GetBirth(config), config.DeviceId);
                            isOnline = true;
                        }
                        catch { Log.Warning("Connecting..."); }
                    }
                    await Task.Delay(5000);
                }
            });

            // ADD THIS FOR DEBUGGING
            node.NodeCommandReceived += e => {
                Log.Debug("DEBUG:NodeCommandReceived: {t}", e.ToString());
      
                return Task.CompletedTask;
            };
            node.StatusMessageReceived += e => {
                Log.Debug("DEBUG: StatusMessageReceived: {t}", e.ToString());

                return Task.CompletedTask;
            };

            // Command Listener
            node.DeviceCommandReceived += async (e) => {
                Log.Debug("Received message: {msg}", e.ToString());
                foreach (var m in e.Metrics)
                {
                    Log.Information("CMD: {n}", m.Name);
                    if (m.Name.Contains("Repair")) twin.Repair();
                    if (m.Name.Contains("Reset")) twin.ResetStats();
                    if (m.Name.Contains("Rebirth")) await node.PublishDeviceBirthMessage(GetBirth(config), config.DeviceId);
                }
            };
            node.Disconnected += e => { isOnline = false; return Task.CompletedTask; };

            

            // Main Loop
            while (true)
            {
                twin.ProcessCycle(config, rng);
                var metrics = twin.GetTelemetry(config, rng)
                    .Where(k => config.MetricsEnabled.GetValueOrDefault(k.Key, true))
                    .Select(k => new Metric(k.Key, GetDT(k.Value), k.Value)).ToList();

                if (isOnline)
                {
                    try
                    {
                        var cached = buffer.Fetch(10);
                        foreach (var c in cached) await node.PublishDeviceData(c.Metrics, config.DeviceId);
                        buffer.Clear(cached.Select(x => x.Id).ToList());
                        await node.PublishDeviceData(metrics, config.DeviceId);
                        Log.Debug("Health: {h:P1} | Cycles: {c}", twin.HealthScore, twin.TotalCycles);
                    }
                    catch { isOnline = false; }
                }
                else
                {
                    buffer.Enqueue(metrics);
                }
                await Task.Delay(config.ScanRateMs);
            }
        }

        static List<Metric> GetBirth(Config c) => new() {
            new Metric("Device Control/Repair", DataType.Boolean, false),
            new Metric("Device Control/Reset Statistics", DataType.Boolean, false),
            new Metric("Device Control/Rebirth", DataType.Boolean, false),
            new Metric("HealthScore", DataType.Double, 100.0),
            new Metric("Status", DataType.String, "Online"),
            new Metric("ProductionCount", DataType.Int64, 0L),
            new Metric("TemperatureC", DataType.Double, 0.0),
            new Metric("HydraulicPressurePsi", DataType.Double, 0.0),
            new Metric("AirPressurePsi", DataType.Double, 0.0),
            new Metric("AirFlowRateLpm", DataType.Double, 0.0),
            new Metric("DefectRate", DataType.Double, 0.0),
            new Metric("PullInCurrentA", DataType.Double, 0.0),
            new Metric("PullInVoltageV", DataType.Double, 0.0),
            new Metric("CoilResistanceOhm", DataType.Double, 0.0),
            new Metric("LeakRateCcMin", DataType.Double, 0.0),
            new Metric("VibrationPeakG", DataType.Double, 0.0)
        };

        static DataType GetDT(object v) => v switch { double => DataType.Double, long => DataType.Int64, bool => DataType.Boolean, _ => DataType.String };
    }
}