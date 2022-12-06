using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Net.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;

namespace Azure.RUS
{
    public class IoTMessageIngestor
    {        
       [FunctionName("IoTMessageIngestor")]
       public static void SaveMessageToDB(
            [IoTHubTrigger("iothub-ehub-iot-hub-ru-22808437-7dd661b999", Connection = "EventHubConnectionString")] string msg,
            ILogger logger)
        {
            if (!string.IsNullOrEmpty(msg))
            {
                logger.LogInformation("Received a message: " + msg);
                
                const string str = "<SQL server connection string>";
                using (SqlConnection conn = new SqlConnection(str))
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
    }
}