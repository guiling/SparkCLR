﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Spark.CSharp.Interop.Ipc;
using Mono.Posix;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp
{
    public class Daemon
    {
        private static Socket listenSocket;
        private static int portNumber = 12345;
        static Daemon() 
        {
            listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            listenSocket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            listenSocket.Listen(Math.Max(1024, int.MaxValue));
            SerDe.Write(Console.OpenStandardOutput(), ((IPEndPoint)listenSocket.LocalEndPoint).Port);
        }

        public static void Run() 
        {
            Task killProcessTask = new Task(() =>
            {
                while (true)
                {
                    Stream inputStream = Console.OpenStandardInput();
                    byte[] bytes = new byte[4];
                    inputStream.Read(bytes, 0, 4);
                    int workerPid = SerDe.ToInt(bytes);
                    Process workerProcess = Process.GetProcessById(workerPid);
                    workerProcess.Kill();
                }
            });

            killProcessTask.Start();

            while (true)
            {
                try
                {
                    List<Socket> listenList = new List<Socket>() { listenSocket };
                    // Only the sockets that contain a connection request
                    // will remain in listenList after Select returns.
                    Socket.Select(listenList, null, null, 1);

                    if (listenList.Count > 0)
                    {
                        Socket socket = listenList[0].Accept();
                        //int childPid = Syscall.fork();
                        //if (childPid == 0)
                        //{
                        //    using (NetworkStream s = new NetworkStream(socket))
                        //    {
                        //        SerDe.Write(s, Process.GetCurrentProcess().Id);
                        //        Worker.Run(socket);
                        //    }
                        //}

                        Process process = new Process();
                        process.StartInfo.UseShellExecute = false;
                        string procDir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
                        process.StartInfo.FileName = Path.Combine(procDir, "CSharpWorker.exe");
                        process.StartInfo.Arguments = string.Format("-port {0}", portNumber);
                        process.Start();
                        SocketInformation sockectInfo = socket.DuplicateAndClose(process.Id);
                        Socket transPortSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

                        transPortSock.Connect(IPAddress.Loopback, portNumber);
                        using (NetworkStream s = new NetworkStream(transPortSock))
                        {
                            SerDe.Write(s, sockectInfo.ProtocolInformation);
                        }

                        transPortSock.Close();

                        portNumber++;
                    }
                }
                catch (SocketException)
                {
                    Environment.Exit(-1);
                    break;
                }
            }
        }
    }
}
