﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Configuration
{
    /// <summary>
    /// Implementation of configuration service that helps getting config settings
    /// to be used in SparkCLR runtime
    /// </summary>
    internal class ConfigurationService : IConfigurationService
    {
        public const string ProcFileName = "CSharpWorkerLauncher.exe";
        public const string CSharpLauncherWorkerPathSettingKey = "CSharpWorkerLauncherPath";
        public const string CSharpBackendPortNumberSettingKey = "CSharpBackendPortNumber";
        public const string SPARKCLR_HOME = "SPARKCLR_HOME";
        public const string SPARK_MASTER = "spark.master";
        public const string CSHARPBACKEND_PORT = "CSHARPBACKEND_PORT";

        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(ConfigurationService));
        private readonly SparkCLRConfiguration configuration;
        private RunMode runMode = RunMode.UNKNOWN; //not used anywhere for now but may come handy in the future

        public int BackendPortNumber
        {
            get
            {
                return configuration.GetPortNumber();
            }
        }

        internal ConfigurationService()
        {
            Assembly entryAssembly = Assembly.GetEntryAssembly();
            if (entryAssembly == null) // happens when instantiate ConfigurationService in unit tests
            {
                entryAssembly = new StackTrace().GetFrames().Last().GetMethod().Module.Assembly;
            }
            var appConfig = ConfigurationManager.OpenExeConfiguration(entryAssembly.Location);
            var sparkMaster = Environment.GetEnvironmentVariable(SPARK_MASTER); //set by CSharpRunner when launching driver process
            if (sparkMaster == null)
            {
                configuration = new SparkCLRDebugConfiguration(appConfig);
                runMode = RunMode.DEBUG;
            }
            else if (sparkMaster.StartsWith("local"))
            {
                configuration = new SparkCLRLocalConfiguration(appConfig);
                runMode = RunMode.LOCAL;
            }
            else if (sparkMaster.StartsWith("spark://"))
            {
                configuration = new SparkCLRConfiguration(appConfig);
                runMode = RunMode.CLUSTER;
            }
            else if (sparkMaster.Equals("yarn-client", StringComparison.OrdinalIgnoreCase) || sparkMaster.Equals("yarn-cluster", StringComparison.OrdinalIgnoreCase))
            {
                configuration = new SparkCLRConfiguration(appConfig);
                runMode = RunMode.YARN;
            }
            else
            {
                throw new NotSupportedException(string.Format("Spark master value {0} not recognized", sparkMaster));
            }

            logger.LogInfo(string.Format("ConfigurationService runMode is {0}", runMode));
        }

        public string GetCSharpWorkerExePath()
        {
            return configuration.GetCSharpWorkerLauncherExePath();
        }

        /// <summary>
        /// Default configuration for SparkCLR jobs.
        /// Works with Standalone cluster mode
        /// May work with YARN or Mesos - needs validation when adding support for YARN/Mesos
        /// </summary>
        private class SparkCLRConfiguration
        {
            protected readonly AppSettingsSection appSettings;
            protected readonly string sparkCLRHome = Environment.GetEnvironmentVariable(SPARKCLR_HOME); //set by sparkclr-submit.cmd
            private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SparkCLRConfiguration));

            internal SparkCLRConfiguration(System.Configuration.Configuration configuration)
            {
                appSettings = configuration.AppSettings;
            }

            /// <summary>
            /// The port number used for communicating with the CSharp external backend worker process.
            /// </summary>
            internal virtual int GetPortNumber()
            {
                int portNo;
                if (!int.TryParse(Environment.GetEnvironmentVariable(CSHARPBACKEND_PORT), out portNo))
                {
                    throw new Exception("Environment variable " + CSHARPBACKEND_PORT + " not set");
                }

                logger.LogInfo("CSharpBackend successfully read from environment variable {0}", CSHARPBACKEND_PORT);
                return portNo;
            }

            /// <summary>
            /// The path of the CSharp external backend worker process.
            /// </summary>
            internal virtual string GetCSharpWorkerLauncherExePath()
            {
                return ProcFileName;
            }
        }

        /// <summary>
        /// Configuration for SparkCLR jobs in ** Local ** mode
        /// Needs some investigation to find out why Local mode behaves
        /// different than standalone cluster mode for the configuration values
        /// overridden here
        /// </summary>
        private class SparkCLRLocalConfiguration : SparkCLRConfiguration
        {
            private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SparkCLRLocalConfiguration));
            internal SparkCLRLocalConfiguration(System.Configuration.Configuration configuration)
                : base(configuration)
            { }

            private string workerPath;
            internal override string GetCSharpWorkerLauncherExePath()
            {
                // SparkCLR jar and driver, worker & dependencies are shipped using Spark file server. 
                // These files are available in the Spark executing directory at executor node.

                if (workerPath != null) return workerPath; // Return cached value

                KeyValueConfigurationElement workerPathConfig = appSettings.Settings[CSharpLauncherWorkerPathSettingKey];
                if (workerPathConfig == null)
                {
                    // Path for the CSharpWorkerLauncher.exe was not specified in App.config
                    // Try to work out where location relative to this class.
                    // Construct path based on well-known file name + directory this class was loaded from.
                    string procDir = Path.GetDirectoryName(GetType().Assembly.Location);
                    workerPath = Path.Combine(procDir, ProcFileName);
                    logger.LogDebug("Using SparkCLR Adapter dll path to construct CSharpWorkerPath : {0}", workerPath);
                }
                else
                {
                    // Explicit path for the CSharpWorkerLauncher.exe was listed in App.config
                    workerPath = workerPathConfig.Value;
                    logger.LogDebug("Using CSharpWorkerLauncherPath value from App.config : {0}", workerPath);
                }

                return workerPath;
            }
        }

        /// <summary>
        /// Configuration mode for debug mode
        /// This configuration exists only to make SparkCLR development & debugging easier
        /// </summary>
        private class SparkCLRDebugConfiguration : SparkCLRLocalConfiguration
        {
            private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SparkCLRDebugConfiguration));
            internal SparkCLRDebugConfiguration(System.Configuration.Configuration configuration)
                : base(configuration)
            {}

            internal override int GetPortNumber()
            {
                KeyValueConfigurationElement portConfig = appSettings.Settings[CSharpBackendPortNumberSettingKey];
                if (portConfig == null)
                {
                    throw new ConfigurationErrorsException(string.Format("Need to set {0} value in App.config for running in DEBUG mode.", CSharpBackendPortNumberSettingKey));
                }
                int cSharpBackendPortNumber = int.Parse(portConfig.Value);
                logger.LogInfo(string.Format("CSharpBackend port number read from app config {0}", cSharpBackendPortNumber));
                return cSharpBackendPortNumber;
            }

            /// <summary>
            /// The full path of the CSharp external backend worker process.
            /// </summary>
            internal override string GetCSharpWorkerLauncherExePath()
            {
                KeyValueConfigurationElement workerPathConfig = appSettings.Settings[CSharpLauncherWorkerPathSettingKey];
                if (workerPathConfig != null)
                {
                    logger.LogInfo("Worker path read from setting {0} in app config", CSharpLauncherWorkerPathSettingKey);
                    return workerPathConfig.Value;
                }
                
                var path = GetSparkCLRArtifactsPath("bin", ProcFileName);
                logger.LogInfo("Worker path {0} constructed using {1} environment variable", path, SPARKCLR_HOME);

                return path;
            }

            private string GetSparkCLRArtifactsPath(string sparkCLRSubFolderName, string fileName)
            {
                var filePath = Path.Combine(sparkCLRHome, sparkCLRSubFolderName, fileName);
                if (!File.Exists(filePath))
                {
                    throw new Exception(string.Format("Path {0} not exists", filePath));
                }
                return filePath;
            }
        }
    }

    public enum RunMode
    {
        UNKNOWN,
        DEBUG, //not a Spark mode but exists for dev debugging purpose
        LOCAL,
        CLUSTER,
        YARN
        //MESOS //not currently supported
    }
}
