// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp
{
    /// <summary>
    /// The main purpose is to be Linux compatible. However, we prefer not to override useDaemon or add a config parameter because it's better to keep the Spark core unchanged.
    /// And of course, using Daemon on Linux can also bring performance advantages.
    /// In short, implementing such a Daemon process is to make SparkCLR work in the same way as pyspark or sparkR, i.e.,
    /// (1) use Daemon to launch worker on Linux,
    /// (2) fall back to launching wokers directly on Windows
    /// </summary>
    class WorkerLauncher
    {
        private const string PYSPARK_WORKER_NAME = "pyspark.worker";
        private const string PYSPARK_DAEMON_NAME = "pyspark.daemon";

        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("the length of args should be 2");
                Environment.Exit(-1);
                return;
            }

            string firstParamter = args[0].Trim();
            if (!firstParamter.Equals("-m"))
            {
                Console.WriteLine("the first parameter of args should be -m");
                Environment.Exit(-1);
                return;
            }

            string runMode = args[1].Trim();
            if (!runMode.Equals(PYSPARK_WORKER_NAME) && !runMode.Equals(PYSPARK_DAEMON_NAME))
            {
                Console.WriteLine(string.Format("the second parameter of args should be either {0} or {1}", PYSPARK_WORKER_NAME, PYSPARK_DAEMON_NAME));
                Environment.Exit(-1);
                return;
            }

            if (runMode.Equals(PYSPARK_WORKER_NAME))
            {
               Worker.Run();
            }
            else
            {
                Daemon.Run();
            }
        }
    }
}
