﻿using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace ML_Training_Engine.Handler
{
    public class Functions
    {
        public static string DatabaseName = "GYM";
        public static string _getStrConn()
        {
            return ConfigurationManager.ConnectionStrings["connEncrypted"].ConnectionString.ToString();
        }
    }
}
