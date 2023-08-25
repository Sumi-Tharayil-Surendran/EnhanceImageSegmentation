using Amazon.Rekognition;
using Amazon.Rekognition.Model;
using Amazon.S3;
using Amazon.S3.Model;
using ML_Training_Engine.Handler;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ML_Training_Engine
{
    class Program
    {
        public static string BucketSource = "retailerimagesegmentation";
        public static string BucketDestination = "tempsegmentaion";
        public static string ProjectVersion = "arn:aws:rekognition:eu-west-1:984198194901:project/EnhanceImageSegmentationinRetail/version/EnhanceImageSegmentationinRetail.2023-08-18T08.27.02/1692336422819";
        public static string ProjectARN = "arn:aws:rekognition:eu-west-1:984198194901:project/EnhanceImageSegmentationinRetail/1690221678130";
        static SqlConnection _conn = new SqlConnection(Functions._getStrConn());
        static void Main(string[] args)
        {
            try
            {
                SetConsoleCtrlHandler(new HandlerRoutine(ConsoleCtrlCheck), true);
                string result = Project_Status_GET();
                //if (result.Equals("RUNNING"))
                //{ return; }
                Console.WriteLine("Get Started");
                DataTable dt = GetQueue();
                Console.WriteLine("Get Came");
                if (dt.Rows.Count == 0)
                {
                    Console.WriteLine("No data");
                    Thread.Sleep(5000);
                    return;
                }
                RekogInitialise();
                LooperRekog(dt);
            }
            catch (Exception ex)
            {
                //LoggerInsert("1", ex.Message, "Main");
            }
            finally
            {
                RekogDispose();
            }
        }
        static void LooperRekog(DataTable dt)
        {
            int count = 1;
            while (dt == null || dt.Rows.Count > 0)
            {
                if (dt != null)
                {
                    try
                    {
                        DoWorkRekog(dt);
                        Console.WriteLine("Batch:" + count.ToString());
                    }
                    catch (Exception ex)
                    {
                        //LoggerInsert("1", ex.Message, "LooperRekog");

                    }
                }

                dt = GetQueue();
                if (dt != null)
                {
                    Console.WriteLine("Get Came" + count.ToString());
                    count++;
                }
            }
        }
 
        static void DoWorkRekog(DataTable dt)
        {
            string SnipID = "", ImageLocation = "";
            //DataTable dt = GetQueue();
            int currentCount = 1;
            foreach (DataRow row in dt.Rows)
            {
                SnipID = row["ID"].ToString();
                ImageLocation = row["ImagePath"].ToString();
                bool ret = Rekognition(ImageLocation, SnipID);
                Console.WriteLine(currentCount.ToString() + "  Snip:" + SnipID + ", Path:" + ImageLocation);
                currentCount++;
            }
        }
        //static void DoWorkBypass(DataTable dt)
        //{
        //    string SnipID = "", ImageLocation = "";
        //    //DataTable dt = GetQueue();
        //    int currentCount = 1;
        //    foreach (DataRow row in dt.Rows)
        //    {
        //        SnipID = row["SnipID"].ToString();
        //        ImageLocation = row["ImageLocation"].ToString();
        //        SnipSegment_ML_Insert_Bulk(SnipID, "0", 0, "0");
        //        Console.WriteLine(currentCount.ToString() + "  Snip:" + SnipID + ", Path:" + ImageLocation);
        //        Console.WriteLine("Blur value:" + SnipID + "=" + val1.ToString());
        //        currentCount++;
        //    }
        //}
        static void RekogInitialise()
        {
            string result = Project_Status_GET();
            Console.WriteLine("RekogInitialise Status:" + result + " Date Time:" + DateTime.Today.ToString("dd-MM-yyyy HH:mm:ss"));
            //Start
            while (!result.Equals("RUNNING"))
            {
                if (result.Equals("STOPPED") || result.Equals("FAILED"))
                    Project_Start();

                Thread.Sleep(5000);
                result = Project_Status_GET();
            }
            Console.WriteLine("RekogInitialise Status:" + result + " Date Time:" + DateTime.Today.ToString("dd-MM-yyyy HH:mm:ss"));
        }
        static void RekogDispose()
        {
            string result = Project_Status_GET();
            //result = Project_Status_GET();
            Console.WriteLine("RekogDispose Status:" + result +
                " Date Time:" + DateTime.Today.ToString("dd-MM-yyyy HH:mm:ss"));
            while (!result.Equals("STOPPED"))
            {
                result = Project_Status_GET();
                if (result.Equals("RUNNING"))
                    Project_Stop();
                Thread.Sleep(5000);
            }
            Console.WriteLine("RekogDispose Status:" + result +
              " Date Time:" + DateTime.Today.ToString("dd-MM-yyyy HH:mm:ss"));
        }

        static string Project_Start()
        {
            try
            {
                AmazonRekognitionClient rekognitionClient = new AmazonRekognitionClient();
                StartProjectVersionRequest obj = new StartProjectVersionRequest();
                obj.ProjectVersionArn = ProjectVersion;
                obj.MinInferenceUnits = 1;
                StartProjectVersionResponse x = rekognitionClient.StartProjectVersion(obj);
                return x.Status;
            }
            catch (Exception ex)
            {
                //LoggerInsert("1", ex.Message, "Project_Start");
                return "";
            }
        }
        static string Project_Stop()
        {
            try
            {
                AmazonRekognitionClient rekognitionClient = new AmazonRekognitionClient();
                StopProjectVersionRequest obj = new StopProjectVersionRequest();
                obj.ProjectVersionArn = ProjectVersion;
                StopProjectVersionResponse x = rekognitionClient.StopProjectVersion(obj);
                return x.Status;
            }
            catch (Exception ex)
            {
                //LoggerInsert("1", ex.Message, "Project_Stop");
                return "";
            }
        }
        static string Project_Status_GET()
        {
            try
            {
                AmazonRekognitionClient rekognitionClient = new AmazonRekognitionClient();
                DescribeProjectVersionsRequest y = new DescribeProjectVersionsRequest();
                y.ProjectArn = ProjectARN;
                y.MaxResults = 10;
                DescribeProjectVersionsResponse x = rekognitionClient.DescribeProjectVersions(y);
                string a = x.ProjectVersionDescriptions[0].Status;
                return a;
            }
            catch (Exception ex)
            {
                //LoggerInsert("1", ex.Message, "Project_Status_GET");
                return "";
            }
        }
        static public bool Rekognition(string imagePath, string snipID)
        {
            try
            {
                string SourcePath = imagePath
                    .Replace("https://retailerimagesegmentation.s3.eu-west-1.amazonaws.com/", "")
                    .Replace("+"," ");
                string DestinationPath = "Sample/" + snipID + ".png";
                CopyingFiles(SourcePath, DestinationPath);
                
                AmazonRekognitionClient rekognitionClient = new AmazonRekognitionClient();
                DetectCustomLabelsRequest detectlabelsRequest = new DetectCustomLabelsRequest()
                {
                    ProjectVersionArn = ProjectVersion,
                    Image = new Image()
                    {
                        S3Object = new Amazon.Rekognition.Model.S3Object()
                        {
                            Name = DestinationPath,
                            Bucket = BucketDestination
                        },
                    },
                    MaxResults = 120,
                    MinConfidence = 5F
                };
                string maxValueSegID = "";
                float currConfidence = 0;
                string confidenceJson = "";
                try
                {
                    DetectCustomLabelsResponse detectLabelsResponse = rekognitionClient.DetectCustomLabels(detectlabelsRequest);
                    confidenceJson = Newtonsoft.Json.JsonConvert.SerializeObject(detectLabelsResponse.CustomLabels);
                    foreach (CustomLabel label in detectLabelsResponse.CustomLabels)
                    {
                        Console.WriteLine("{0}: {1}", label.Name, label.Confidence);
                        if (currConfidence < label.Confidence)
                        {
                            currConfidence = label.Confidence;
                            maxValueSegID = label.Name;
                         
                        }

                    }
                }
                catch (Exception ex)
                {
                }
                Console.WriteLine("Selected : {0}: {1}", maxValueSegID, currConfidence);
                string status = "1";
                SnipSegment_ML_Insert_Bulk(snipID, maxValueSegID, confidenceJson, status);
                DeleteFile(DestinationPath);
                return true;
            }
            catch (Exception ex)
            {
                //LoggerInsert("1", ex.Message, "Rekognition");
                return false;
            }
        }
        static public bool CopyingFiles(string source, string destination)
        {
            AmazonS3Client s3Client = new AmazonS3Client();
            try
            {
                CopyObjectRequest request = new CopyObjectRequest
                {
                    SourceBucket = BucketSource,
                    SourceKey = source,
                    DestinationBucket = BucketDestination,
                    DestinationKey = destination,
                    CannedACL = S3CannedACL.PublicRead,
                };
                CopyObjectResponse response = s3Client.CopyObject(request);
                return true;
            }
            catch (Exception e)
            {
                //LoggerInsert("1", e.Message, "CopyingFiles");
                return false;
            }
        }
        static void DeleteFile(string key)
        {
            AmazonS3Client s3Client = new AmazonS3Client();
            DeleteObjectRequest request = new DeleteObjectRequest();
            request.BucketName = BucketDestination;
            request.Key = key;
            s3Client.DeleteObject(request);
            s3Client.Dispose();

        }
        static private DataTable GetQueue()
        {
            try
            {
                string sqlStr = string.Format(@"IS_ProductImage_Pending_Get");
                SqlCommand command = new SqlCommand(sqlStr, _conn);
                command.CommandType = CommandType.StoredProcedure;
                command.CommandTimeout = 0;
                SqlDataAdapter da = new SqlDataAdapter();
                DataSet ds = new DataSet();
                da.SelectCommand = command;

                if (_conn != null && _conn.State == ConnectionState.Closed)
                {
                    _conn.Open();
                }
                da.Fill(ds);
                if (_conn != null && _conn.State == ConnectionState.Open)
                {
                    _conn.Close();
                }

                return ds.Tables[0];
            }
            catch (Exception ex)
            {
                //LoggerInsert("1", ex.Message, "GetQueue");
                return null;
            }
        }
        static private void SnipSegment_ML_Insert_Bulk(string snipID, string segID,
            string confidence, string status)
        {
            try
            {
                string sqlStr = string.Format(@"IS_ProductImage_Update");
                SqlCommand command = new SqlCommand(sqlStr, _conn);
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.AddWithValue("@ID", snipID);
                command.Parameters.AddWithValue("@SegmentName", segID);
                command.Parameters.AddWithValue("@ConfidenceJson", confidence);
                command.Parameters.AddWithValue("@Status", status);
                command.CommandTimeout = 0;
                if (_conn != null && _conn.State == ConnectionState.Closed)
                {
                    _conn.Open();
                }
                command.ExecuteNonQuery();
                if (_conn != null && _conn.State == ConnectionState.Open)
                {
                    _conn.Close();
                }
            }
            catch (Exception ex)
            {
               // LoggerInsert("1", ex.Message, "SnipSegment_ML_Insert_Bulk");
            }
        }
        //static private void LoggerInsert(string isException, string message, string method)
        //{
        //    try
        //    {
        //        string sqlStr = string.Format(@"[cat].[MLSegment].[LoggerInsert]");
        //        SqlCommand command = new SqlCommand(sqlStr, _conn);
        //        command.CommandType = CommandType.StoredProcedure;
        //        command.Parameters.AddWithValue("@IsException", isException);
        //        command.Parameters.AddWithValue("@Message", message);
        //        command.Parameters.AddWithValue("@MethodName", method);
        //        command.CommandTimeout = 0;
        //        if (_conn != null && _conn.State == ConnectionState.Closed)
        //        {
        //            _conn.Open();
        //        }
        //        command.ExecuteNonQuery();
        //        if (_conn != null && _conn.State == ConnectionState.Open)
        //        {
        //            _conn.Close();
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //    }
        //}
        private static bool ConsoleCtrlCheck(CtrlTypes ctrlType)
        {
            switch (ctrlType)
            {
                case CtrlTypes.CTRL_C_EVENT:
                    Console.WriteLine("CTRL+C received!");
                    break;
                case CtrlTypes.CTRL_BREAK_EVENT:
                    Console.WriteLine("CTRL+BREAK received!");
                    break;
                case CtrlTypes.CTRL_CLOSE_EVENT:
                    Console.WriteLine("Program being closed!");
                    RekogDispose();
                    break;
                case CtrlTypes.CTRL_LOGOFF_EVENT:

                case CtrlTypes.CTRL_SHUTDOWN_EVENT:
                    Console.WriteLine("User is logging off!");
                    break;
            }

            return true;

        }
        #region unmanaged
        [DllImport("Kernel32")]
        public static extern bool SetConsoleCtrlHandler(HandlerRoutine Handler, bool Add);
        public delegate bool HandlerRoutine(CtrlTypes CtrlType);
        public enum CtrlTypes
        {
            CTRL_C_EVENT = 0,
            CTRL_BREAK_EVENT,
            CTRL_CLOSE_EVENT,
            CTRL_LOGOFF_EVENT = 5,
            CTRL_SHUTDOWN_EVENT
        }
        #endregion

        
    }
}
