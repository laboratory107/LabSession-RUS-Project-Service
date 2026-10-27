using System;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using System.Text.Json;

namespace FOI.RUS;

public class Ambient
{
    public double Temperature { get; set; }
    public double Humidity { get; set; }
    public int Light { get; set; }
}

public class MessageBody
{
    public Sentiment Sentiment { get; set; }
    public Ambient Ambient { get; set; }
    public int DeviceID { get; set; }
}

public class Sentiment
{
    public int Status { get; set; }
}

public class IoTReceiver(ILogger<IoTReceiver> logger)
{
    private const string eventHubName = "<event-hub-name-from-iot-hub-settings>";
    private const string eventHubConnectionString = "IotHubConnectionString";
    private static readonly string? databaseConnectionString = Environment.GetEnvironmentVariable("SqlConnectionString");

    [Function(nameof(IoTReceiver))]
    public void Run([EventHubTrigger(eventHubName, Connection = eventHubConnectionString)] EventData[] events)
    {
        using SqlConnection conn = new(databaseConnectionString);
        conn.Open();

        foreach (EventData @event in events)
        {
            string msg = @event.EventBody.ToString();
            ProcessTelemetryMessage(conn, msg);
        }

        conn.Close();
    }

    private void ProcessTelemetryMessage(SqlConnection conn, string msg)
    {
        logger.LogInformation($"Received telemetry message: {msg}");
        var messageBody = JsonSerializer.Deserialize<MessageBody>(msg);

        if (messageBody != null)
        {
            using var transaction = conn.BeginTransaction();
            string insertQuery = @"
                                INSERT INTO Measurement(DateTime, Temperature, Humidity, Luminance, DeviceID)
                                VALUES (GETDATE(), @Temperature, @Humidity, @Luminance, @DeviceID);";

            using (SqlCommand cmd = new(insertQuery, conn, transaction))
            {
                cmd.Parameters.AddWithValue("Temperature", messageBody.Ambient.Temperature);
                cmd.Parameters.AddWithValue("Humidity", messageBody.Ambient.Humidity);
                cmd.Parameters.AddWithValue("Luminance", messageBody.Ambient.Light);
                cmd.Parameters.AddWithValue("DeviceID", messageBody.DeviceID);

                cmd.ExecuteNonQuery();
            }      

            transaction.Commit();   

            logger.LogInformation("Telemetry data saved successfully"); 
        } 
        else
        {
            logger.LogError("Received a telemetry message in invalid format...");
        }
    }
}