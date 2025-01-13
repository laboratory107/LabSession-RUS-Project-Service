using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;

namespace Azure.RUS
{
    class MessageBody
    {
        public Sentiment Sentiment { get; set; }
        public Ambient Ambient { get; set; }
        public int DeviceID { get; set; }
    }

    class Sentiment
    {
        public short Status { get; set; }
    }

    class Ambient
    {
        public float Temperature { get; set; }
        public float Humidity { get; set; }
        public int Light { get; set; }
    }

    public class IoTMessageIngestor
    {
        const string eventHubName = "<eventHubName>";
        const string databaseConnectionString = "<dbConnString>";

        [FunctionName("SaveTelemetryToDBFunction")]
        public static void SaveTelemetryToDB(
            [IoTHubTrigger(eventHubName, Connection = "EventHubConnectionString")] string msg,
            ILogger logger)
        {
            if (!string.IsNullOrEmpty(msg))
            {
                using SqlConnection conn = new SqlConnection(databaseConnectionString);
                conn.Open();

                if (msg.StartsWith("Device"))
                {
                    ProcessIsAliveMessage(conn, msg, logger);
                }
                else
                {
                    ProcessTelemetryMessage(conn, msg, logger);
                }

                conn.Close();
            }
        }

        private static void ProcessIsAliveMessage(SqlConnection conn, string msg, ILogger logger)
        {
            logger.LogInformation($"Info: Received IsAlive message: {msg}");

            string query = "UPDATE Device SET LastOnline = GETDATE() WHERE Name = @DeviceName";
            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.Add(new SqlParameter("DeviceName", msg));
                int rows = cmd.ExecuteNonQuery();
                logger.LogInformation($"{rows} rows were updated for IsAlive message.");
            }
        }

        private static void ProcessTelemetryMessage(SqlConnection conn, string msg, ILogger logger)
        {
            logger.LogInformation($"Info: Received telemetry message: {msg}");
            var messageBody = JsonConvert.DeserializeObject<MessageBody>(msg);

            if (messageBody == null)
            {
                logger.LogWarning("Message deserialization returned null.");
                return;
            }

            using (var transaction = conn.BeginTransaction())
            {
                string insertAmbientQuery = @"
                    INSERT INTO Measurement (DateTime, Temperature, Humidity, Luminance, DeviceID) 
                    VALUES (GETDATE(), @Temperature, @Humidity, @Luminance, @DeviceID);";

                string insertSentimentQuery = @"
                    INSERT INTO OwnerSentiment (Status, DateTime, DeviceID) 
                    VALUES (@Status, GETDATE(), @DeviceID);";

                using (SqlCommand cmd = new SqlCommand(insertAmbientQuery, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("Temperature", messageBody.Ambient.Temperature);
                    cmd.Parameters.AddWithValue("Humidity", messageBody.Ambient.Humidity);
                    cmd.Parameters.AddWithValue("Luminance", messageBody.Ambient.Light);
                    cmd.Parameters.AddWithValue("DeviceID", messageBody.DeviceID);

                    cmd.ExecuteNonQuery();
                }

                using (SqlCommand cmd = new SqlCommand(insertSentimentQuery, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("Status", messageBody.Sentiment.Status);
                    cmd.Parameters.AddWithValue("DeviceID", messageBody.DeviceID);

                    cmd.ExecuteNonQuery();
                }

                transaction.Commit();
                logger.LogInformation("Telemetry data saved successfully.");
            }
        }
    }
}