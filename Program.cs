using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Configuration;

namespace DeviceInfo_RecordHistory
{
    internal class Program
    {
        public static string sourceConnectionString = ConfigurationManager.AppSettings["SourceConnectionString"];
        public static string destinationConnectionString = ConfigurationManager.AppSettings["DestinationConnectionString"];
        public static string sourceTableName = ConfigurationManager.AppSettings["SourceTableName"];
        public static string destinationTableName = ConfigurationManager.AppSettings["DestinationTableName"];
        static void Main(string[] args)
        {

            // Ensure destination table exists
            EnsureTableExists(destinationConnectionString, destinationTableName);

            // Synchronize data from source table to destination table
            SyncData(sourceConnectionString, destinationConnectionString);

        }

        static void EnsureTableExists(string connectionString, string tableName)
        {
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                connection.Open();
                MySqlCommand command = new MySqlCommand($@"CREATE TABLE IF NOT EXISTS {tableName} (
                DEVICE_ID VARCHAR(32),
                TERM_ID VARCHAR(32),
                DEPT_ID VARCHAR(32),
                TYPE_ID VARCHAR(32),
                BRAND_ID VARCHAR(32),
                MODEL_ID VARCHAR(32),
                TERM_SEQ VARCHAR(40),
                COUNTER_CODE VARCHAR(40),
                TERM_IP VARCHAR(40),
                STATUS VARCHAR(10),
                TERM_NAME VARCHAR(100),
                TERM_ADDR VARCHAR(200),
                TERM_LOCATION VARCHAR(200) CHARACTER SET utf8mb4,
                TERM_ZONE VARCHAR(30) CHARACTER SET utf8mb4,
                CONTROL_BY VARCHAR(50) CHARACTER SET utf8mb4,
                REPLENISH_BY VARCHAR(50) CHARACTER SET utf8mb4,
                POST VARCHAR(100),
                INSTALL_DATE VARCHAR(32),
                ACTIVE_DATE VARCHAR(32),
                SERVICE_TYPE VARCHAR(100),
                INSTALL_TYPE VARCHAR(20),
                LAYOUT_TYPE VARCHAR(20),
                MAN_ID VARCHAR(32),
                SERVICEMAN_ID VARCHAR(32),
                COMPANY_ID VARCHAR(32),
                COMPANY_NAME VARCHAR(200),
                SERVICE_BEGINDATE VARCHAR(32),
                SERVICE_ENDDATE VARCHAR(32),
                SERVICE_YEARS VARCHAR(10),
                IS_CCTV VARCHAR(10),
                IS_UPS VARCHAR(10),
                IS_INTERNATIONAL VARCHAR(10),
                BUSINESS_BEGINTIME VARCHAR(20),
                BUSINESS_ENDTIME VARCHAR(20),
                IS_VIP VARCHAR(10),
                AREA_ID VARCHAR(32),
                AREA_ADDR VARCHAR(32),
                FUNCTION_TYPE VARCHAR(32),
                LONGITUDE VARCHAR(32),
                LATITUDE VARCHAR(32),
                PROVINCE VARCHAR(100),
                LOT_TYPE VARCHAR(100),
                AUDITING VARCHAR(10),
                CURRENT_IP VARCHAR(32),
                VERSION_ATMC VARCHAR(256),
                VERSION_SP VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_bin,
                VERSION_AGENT VARCHAR(100),
                VERSION_MB VARCHAR(100),
                FLAG_XFS VARCHAR(10),
                FLAG_EJ VARCHAR(10),
                FLAG_FSN VARCHAR(10),
                EJ_FILES VARCHAR(200),
                FSN_PATH VARCHAR(200),
                TASK_PARA VARCHAR(1000),
                VERSION_AD VARCHAR(100),
                MODIFY_USERID VARCHAR(40),
                MODIFY_DATE DATETIME,
                ADD_USERID VARCHAR(40),
                ADD_DATE DATETIME,
                ASSET_NO VARCHAR(40),
                CASH_BOX_NUM VARCHAR(40),
                SERVICE_SMS_TYPE VARCHAR(2),
                EJ_OPEN_DATE VARCHAR(32),
                ATMC_UPDATE_TIME DATETIME,
                SP_UPDATE_TIME DATETIME,
                AGENT_UPDATE_TIME DATETIME,
                VERSION_NV VARCHAR(100),
                NV_UPDATE_TIME DATETIME,
                VERSION_MAIN VARCHAR(100),
                MAIN_UPDATE_TIME DATETIME
                )", connection);

                command.ExecuteNonQuery();
            }
        }

        static void SyncData(string sourceConnectionString, string destinationConnectionString)
        {
            // Retrieve data from source table
            DataTable sourceData = GetData(sourceConnectionString, sourceTableName);

            // Retrieve existing data from destination table
            DataTable destinationData = GetData(destinationConnectionString, destinationTableName);

            // Compare and insert rows from sourceData into destinationData
            CompareAndInsertOrUpdate(sourceData, destinationData, destinationConnectionString, destinationTableName);
        }

        static DataTable GetData(string connectionString, string tableName)
        {
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                connection.Open();
                MySqlDataAdapter adapter = new MySqlDataAdapter($"SELECT * FROM {tableName}", connection);
                DataTable data = new DataTable();
                adapter.Fill(data);
                return data;
            }
        }

        static void CompareAndInsertOrUpdate(DataTable sourceData, DataTable destinationData, string connectionString, string destinationTableName)
        {
            foreach (DataRow sourceRow in sourceData.Rows)
            {
                string deviceId = sourceRow["DEVICE_ID"].ToString();
                string termId = sourceRow["TERM_ID"].ToString();
                string serviceBeginDate = sourceRow["SERVICE_BEGINDATE"].ToString();
                // Check if the row exists in destinationData
                DataRow[] matchingRows = destinationData.Select($"DEVICE_ID = '{deviceId}' AND TERM_ID = '{termId}' AND SERVICE_BEGINDATE = '{serviceBeginDate}'");

                if (matchingRows.Length == 0)
                {
                    // If the row doesn't exist, insert it into the destination table
                    InsertRow(destinationTableName, sourceRow, connectionString);
                    Console.WriteLine($"Inserted row with DEVICE_ID {deviceId} and TERM_ID {termId} into {destinationTableName}");
                }
                else
                {
                    // If the row exists, check if any other columns have different values
                    DataRow destinationRow = matchingRows[0];
                    bool rowNeedsUpdate = false;

                    foreach (DataColumn column in sourceData.Columns)
                    {
                        if (!sourceRow[column.ColumnName].Equals(destinationRow[column.ColumnName]))
                        {
                            rowNeedsUpdate = true;
                            break;
                        }
                    }

                    if (rowNeedsUpdate)
                    {
                        // If the row needs update, update it in the destination table
                        UpdateRow(destinationTableName, sourceRow, connectionString);
                        Console.WriteLine($"Updated row with DEVICE_ID {deviceId} and TERM_ID {termId} in {destinationTableName}");
                    }
                }
            }
        }

        static void InsertRow(string tableName, DataRow row, string connectionString)
        {
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                connection.Open();
                string[] values = row.ItemArray.Select(value =>
                {
                    if (value == null || value == DBNull.Value)
                    {
                        return "NULL";
                    }
                    else if (value is string)
                    {
                        return $"'{value}'";
                    }
                    else if (value is DateTime)
                    {
                        DateTime dateTimeValue = (DateTime)value;
                        return $"'{dateTimeValue:yyyy-MM-dd HH:mm:ss}'";
                    }
                    else
                    {
                        return value.ToString();
                    }
                }).ToArray();
                string query = $"INSERT INTO {tableName} VALUES ({string.Join(",", values)})";
                MySqlCommand command = new MySqlCommand(query, connection);

                command.ExecuteNonQuery();
            }
        }

        static void UpdateRow(string tableName, DataRow row, string connectionString)
        {
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                connection.Open();
                MySqlCommand command = new MySqlCommand($"UPDATE {tableName} SET {GetUpdateValues(row)} WHERE DEVICE_ID = '{row["DEVICE_ID"]}' AND TERM_ID = '{row["TERM_ID"]}'", connection);
                command.ExecuteNonQuery();
            }
        }

        static string GetUpdateValues(DataRow row)
        {
            StringBuilder updateValuesBuilder = new StringBuilder();
            foreach (DataColumn column in row.Table.Columns)
            {
                string columnName = column.ColumnName;
                string columnValue = GetColumnValue(row[columnName]);
                updateValuesBuilder.Append($"{columnName} = {columnValue}, ");
            }
            // Remove the trailing comma and space
            string updateValues = updateValuesBuilder.ToString().TrimEnd(',', ' ');
            return updateValues;
        }

        static string GetColumnValue(object value)
        {
            if (value == null || value == DBNull.Value)
            {
                return "NULL";
            }
            else if (value is string)
            {
                return $"'{value}'";
            }
            else if (value is DateTime)
            {
                DateTime dateTimeValue = (DateTime)value;
                return $"'{dateTimeValue:yyyy-MM-dd HH:mm:ss}'";
            }
            else
            {
                return value.ToString();
            }
        }
    }
}
