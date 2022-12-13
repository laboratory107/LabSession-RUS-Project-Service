using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Net.Http;
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

       [FunctionName("AliveStatusFunction")]
       public static void SaveAliveStatusToDB(
            [IoTHubTrigger(eventHubName, Connection = "EventHubConnectionString")] string msg,
            ILogger logger)
        {
            if (!string.IsNullOrEmpty(msg) && msg.StartsWith("Device"))
            {
                logger.LogInformation("Received an IsAlive message: " + msg);
                
                using (SqlConnection conn = new SqlConnection(databaseConnectionString))
                {
                    conn.Open();
                    string query = "UPDATE Device SET LastOnline = getdate() WHERE Name=@DeviceName";
                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {
                        cmd.Parameters.Add(new SqlParameter("DeviceName", msg));

                        var rows = cmd.ExecuteNonQuery();
                        logger.LogInformation($"{rows} rows were updated");
                    }
                    conn.Close();
                }
            }
        }

        [FunctionName("SaveTelemetryToDBFunction")]
        public static void SaveTelemetryToDB(
            [EventHubTrigger(eventHubName, Connection = "EventHubConnectionString")] string msg,
            ILogger logger)
        {
            logger.LogInformation("Received an IsAlive message: " + msg);

            if (!string.IsNullOrEmpty(msg) && !msg.StartsWith("Device"))
            {
                logger.LogInformation("Info: Received telemetry message" + msg);
                MessageBody messageBody = JsonConvert.DeserializeObject<MessageBody>(msg);

                using (SqlConnection conn = new SqlConnection(databaseConnectionString))
                {
                    conn.Open();

                    string insertAmbientQuery = "INSERT INTO Measurement (DateTime, Temperature, Humidity, Luminance, DeviceID) VALUES (GETDATE(), @Temperature, @Humidity, @Luminance, @DeviceID);";
                    string insertSentimentQuery = "INSERT INTO OwnerSentiment (Status, DateTime, DeviceID) VALUES (@Status, GETDATE(), @DeviceID);";
                    using (SqlCommand cmd = new SqlCommand(insertAmbientQuery , conn))
                    {
                        cmd.Parameters.Add(new SqlParameter("DeviceID", messageBody.DeviceID));
                        cmd.Parameters.Add(new SqlParameter("Temperature", messageBody.Ambient.Temperature));
                        cmd.Parameters.Add(new SqlParameter("Humidity", messageBody.Ambient.Humidity));
                        cmd.Parameters.Add(new SqlParameter("Luminance", messageBody.Ambient.Light));
                       
                        var rows = cmd.ExecuteNonQuery();
                        logger.LogInformation($"{rows} rows were updated");
                    }

                    using (SqlCommand cmd = new SqlCommand(insertSentimentQuery , conn))
                    {
                        cmd.Parameters.Add(new SqlParameter("Status", messageBody.Sentiment.Status));
                        cmd.Parameters.Add(new SqlParameter("DeviceID", messageBody.DeviceID));

                        var rows = cmd.ExecuteNonQuery();
                        logger.LogInformation($"{rows} rows were updated");
                    }

                    conn.Close();
                }
            }
        }
    }
}