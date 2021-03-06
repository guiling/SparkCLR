﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Sql;
using Razorvine.Pickle;

namespace Microsoft.Spark.CSharp
{
    /// <summary>
    /// Worker implementation for SparkCLR. The implementation is identical to the 
    /// worker used in PySpark. The RDD implementation to fork an external process
    /// and pipe data in and out between JVM & the other runtime is already implemented in PySpark.
    /// SparkCLR uses the same design and implementation of PythonRDD (CSharpRDD extends PythonRDD).
    /// So the worker behavior is also the identical between PySpark and SparkCLR.
    /// </summary>
    public class Worker
    {
        private static readonly DateTime UnixTimeEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private static ILoggerService logger;
       
        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("the length of args should be 2");
                Environment.Exit(-1);
                return;
            }

            string firstParamter = args[0].Trim();
            if (!firstParamter.Equals("-port"))
            {
                Console.WriteLine("the first parameter of args should be -port");
                Environment.Exit(-1);
                return;
            }

            string strPort = args[1].Trim();
            int portNumber = 0;
            if (!int.TryParse(strPort, out portNumber))
            {
                Console.WriteLine("the second parameter of args should be an integer");
                Environment.Exit(-1);
                return;
            }

            Socket listenSocket = null;
            Socket clientSocket = null;
            SocketInformation socketInfo;
            try
            {
                listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                listenSocket.Bind(new IPEndPoint(IPAddress.Loopback, portNumber));
                listenSocket.Listen(5);
                clientSocket = listenSocket.Accept();

                // receive protocolinfo of the duplicated socket
                byte[] recvBytes = new byte[10000];
                int count = 0;
                using (NetworkStream s = new NetworkStream(clientSocket))
                {
                    count = s.Read(recvBytes, 0, 10000);
                }

                socketInfo = new SocketInformation();
                byte[] protocolInfo = new byte[count];
                Array.Copy(recvBytes, protocolInfo, count);
                socketInfo.ProtocolInformation = protocolInfo;
                socketInfo.Options = SocketInformationOptions.Connected;
            }
            finally
            {
                if (clientSocket != null)
                {
                    clientSocket.Close();
                }

                if (listenSocket != null)
                {
                    listenSocket.Close();
                }
            }

            string envVar = Environment.GetEnvironmentVariable("SPARK_REUSE_WORKER"); // this envVar is set in JVM side
            bool sparkReuseWorker = false;
            if ((envVar != null) && envVar.Equals("1"))
            {
                sparkReuseWorker = true;
            }

            Socket socket = new Socket(socketInfo);

            // Acknowledge that the fork was successful
            using (NetworkStream s = new NetworkStream(socket))
            {
                SerDe.Write(s, Process.GetCurrentProcess().Id);
            }

            while (true)
            {
                int exitcode = Run(socket);
                if (!sparkReuseWorker || exitcode == -1)
                {
                    break;
                }
            }
            
            // wait for server to complete, otherwise server gets 'connection reset' exception
            // Use SerDe.ReadBytes() to detect java side has closed socket properly
            // ReadBytes() will block until the socket is closed
            using (NetworkStream s = new NetworkStream(socket))
            {
                logger.LogInfo("ReadBytes begin");
                SerDe.ReadBytes(s);
                logger.LogInfo("ReadBytes end");
            }

            if (socket != null)
            {
                socket.Close();
            }
        }

        public static int Run(Socket sock = null)
        {
            // if there exists exe.config file, then use log4net
            if (File.Exists(AppDomain.CurrentDomain.SetupInformation.ConfigurationFile))
            {
                LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance);
            }

            logger = LoggerServiceFactory.GetLogger(typeof(Worker));

            if (sock == null)
            {
                try
                {  
                    PrintFiles();
                    int javaPort = int.Parse(Console.ReadLine());
                    logger.LogDebug("java_port: " + javaPort);
                    sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    sock.Connect(IPAddress.Loopback, javaPort);
                }
                catch (Exception e)
                {
                    logger.LogError("CSharpWorker failed with exception:");
                    logger.LogException(e);
                    Environment.Exit(-1);
                }
            }

            using (NetworkStream s = new NetworkStream(sock))
            {
                try
                {
                    DateTime bootTime = DateTime.UtcNow;

                    int splitIndex = SerDe.ReadInt(s);
                    logger.LogDebug("split_index: " + splitIndex);
                    if (splitIndex == -1)
                        Environment.Exit(-1);

                    string ver = SerDe.ReadString(s);
                    logger.LogDebug("ver: " + ver);

                    //// initialize global state
                    //shuffle.MemoryBytesSpilled = 0
                    //shuffle.DiskBytesSpilled = 0

                    // fetch name of workdir
                    string sparkFilesDir = SerDe.ReadString(s);
                    logger.LogDebug("spark_files_dir: " + sparkFilesDir);
                    //SparkFiles._root_directory = sparkFilesDir
                    //SparkFiles._is_running_on_worker = True

                    // fetch names of includes - not used //TODO - complete the impl
                    int numberOfIncludesItems = SerDe.ReadInt(s);
                    logger.LogDebug("num_includes: " + numberOfIncludesItems);

                    if (numberOfIncludesItems > 0)
                    {
                        for (int i = 0; i < numberOfIncludesItems; i++)
                        {
                            string filename = SerDe.ReadString(s);
                        }
                    }

                    // fetch names and values of broadcast variables
                    int numBroadcastVariables = SerDe.ReadInt(s);
                    logger.LogDebug("num_broadcast_variables: " + numBroadcastVariables);

                    if (numBroadcastVariables > 0)
                    {
                        for (int i = 0; i < numBroadcastVariables; i++)
                        {
                            long bid = SerDe.ReadLong(s);
                            if (bid >= 0)
                            {
                                string path = SerDe.ReadString(s);
                                Broadcast.broadcastRegistry[bid] = new Broadcast(path);
                            }
                            else
                            {
                                bid = -bid - 1;
                                Broadcast.broadcastRegistry.Remove(bid);
                            }
                        }
                    }

                    Accumulator.accumulatorRegistry.Clear();

                    int lengthOfCommandByteArray = SerDe.ReadInt(s);
                    logger.LogDebug("command length: " + lengthOfCommandByteArray);

                    IFormatter formatter = new BinaryFormatter();

                    if (lengthOfCommandByteArray > 0)
                    {
                        Stopwatch commandProcessWatch = new Stopwatch();
                        Stopwatch funcProcessWatch = new Stopwatch();
                        commandProcessWatch.Start();

                        int rddId = SerDe.ReadInt(s);
                        int stageId = SerDe.ReadInt(s);
                        int partitionId = SerDe.ReadInt(s);
                        logger.LogInfo(string.Format("rddInfo: rddId {0}, stageId {1}, partitionId {2}", rddId, stageId, partitionId));

                        string deserializerMode = SerDe.ReadString(s);
                        logger.LogDebug("Deserializer mode: " + deserializerMode);

                        string serializerMode = SerDe.ReadString(s);
                        logger.LogDebug("Serializer mode: " + serializerMode);

                        byte[] command = SerDe.ReadBytes(s);

                        logger.LogDebug("command bytes read: " + command.Length);
                        var stream = new MemoryStream(command);

                        var workerFunc = (CSharpWorkerFunc)formatter.Deserialize(stream);
                        var func = workerFunc.Func;
                        //logger.LogDebug("------------------------ Printing stack trace of workerFunc for ** debugging ** ------------------------------");
                        //logger.LogDebug(workerFunc.StackTrace);
                        //logger.LogDebug("--------------------------------------------------------------------------------------------------------------");
                        DateTime initTime = DateTime.UtcNow;
                        int count = 0;

                        // here we use low level API because we need to get perf metrics
                        WorkerInputEnumerator inputEnumerator = new WorkerInputEnumerator(s, deserializerMode);
                        IEnumerable<dynamic> inputEnumerable = Enumerable.Cast<dynamic>(inputEnumerator);
                        funcProcessWatch.Start();
                        IEnumerable<dynamic> outputEnumerable = func(splitIndex, inputEnumerable);
                        var outputEnumerator = outputEnumerable.GetEnumerator();
                        funcProcessWatch.Stop();
                        while (true)
                        {
                            funcProcessWatch.Start();
                            bool hasNext = outputEnumerator.MoveNext();
                            funcProcessWatch.Stop();
                            if (!hasNext)
                            {
                                break;
                            }

                            funcProcessWatch.Start();
                            var message = outputEnumerator.Current;
                            funcProcessWatch.Stop();

                            if (object.ReferenceEquals(null, message))
                            {
                                continue;
                            }

                            byte[] buffer;
                            switch ((SerializedMode)Enum.Parse(typeof(SerializedMode), serializerMode))
                            {
                                case SerializedMode.None:
                                    buffer = message as byte[];
                                    break;

                                case SerializedMode.String:
                                    buffer = SerDe.ToBytes(message as string);
                                    break;

                                case SerializedMode.Row:
                                    Pickler pickler = new Pickler();
                                    buffer = pickler.dumps(new ArrayList { message });
                                    break;

                                default:
                                    try
                                    {
                                        var ms = new MemoryStream();
                                        formatter.Serialize(ms, message);
                                        buffer = ms.ToArray();
                                    }
                                    catch (Exception)
                                    {
                                        logger.LogError("Exception serializing output");
                                        logger.LogError("{0} : {1}", message.GetType().Name, message.GetType().FullName);

                                        throw;
                                    }
                                    break;
                            }

                            count++;
                            SerDe.Write(s, buffer.Length);
                            SerDe.Write(s, buffer);
                        }

                        //TODO - complete the impl
                        logger.LogDebug("Output entries count: " + count);
                        //if profiler:
                        //    profiler.profile(process)
                        //else:
                        //    process()

                        DateTime finish_time = DateTime.UtcNow;
                        const string format = "MM/dd/yyyy hh:mm:ss.fff tt";
                        logger.LogDebug(string.Format("bootTime: {0}, initTime: {1}, finish_time: {2}",
                            bootTime.ToString(format), initTime.ToString(format), finish_time.ToString(format)));
                        SerDe.Write(s, (int)SpecialLengths.TIMING_DATA);
                        SerDe.Write(s, ToUnixTime(bootTime));
                        SerDe.Write(s, ToUnixTime(initTime));
                        SerDe.Write(s, ToUnixTime(finish_time));

                        SerDe.Write(s, 0L); //shuffle.MemoryBytesSpilled  
                        SerDe.Write(s, 0L); //shuffle.DiskBytesSpilled

                        commandProcessWatch.Stop();

                        // log statistics
                        inputEnumerator.LogStatistic();
                        logger.LogInfo(string.Format("func process time: {0}", funcProcessWatch.ElapsedMilliseconds));
                        logger.LogInfo(string.Format("command process time: {0}", commandProcessWatch.ElapsedMilliseconds));
                    }
                    else
                    {
                        logger.LogWarn("lengthOfCommandByteArray = 0. Nothing to execute :-(");
                    }

                    // Mark the beginning of the accumulators section of the output
                    SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);

                    SerDe.Write(s, Accumulator.accumulatorRegistry.Count);
                    foreach (var item in Accumulator.accumulatorRegistry)
                    {
                        var ms = new MemoryStream();
                        var value = item.Value.GetType().GetField("value", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(item.Value);
                        logger.LogDebug(string.Format("({0}, {1})", item.Key, value));
                        formatter.Serialize(ms, new KeyValuePair<int, dynamic>(item.Key, value));
                        byte[] buffer = ms.ToArray();
                        SerDe.Write(s, buffer.Length);
                        SerDe.Write(s, buffer);
                    }

                    int end = SerDe.ReadInt(s);

                    // check end of stream
                    if (end == (int)SpecialLengths.END_OF_STREAM)
                    {
                        SerDe.Write(s, (int)SpecialLengths.END_OF_STREAM);
                        logger.LogDebug("END_OF_STREAM: " + (int)SpecialLengths.END_OF_STREAM);
                    }
                    else
                    {
                        // write a different value to tell JVM to not reuse this worker
                        SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);
                        Environment.Exit(-1);
                    }
                    s.Flush();

                    // log bytes read and write
                    logger.LogDebug(string.Format("total read bytes: {0}", SerDe.totalReadNum));
                    logger.LogDebug(string.Format("total write bytes: {0}", SerDe.totalWriteNum));
                }
                catch (Exception e)
                {
                    logger.LogError(e.ToString());
                    try
                    {
                        SerDe.Write(s, e.ToString());
                    }
                    catch (IOException)
                    {
                        // JVM close the socket
                    }
                    catch (Exception ex)
                    {
                        logger.LogError("CSharpWorker failed with exception:");
                        logger.LogException(ex);
                    }
                    Environment.Exit(-1);
                    return -1;
                }

                return 0;
            }
        }

        private static void PrintFiles()
        {
            logger.LogDebug("Files available in executor");
            var driverFolder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            var files = Directory.EnumerateFiles(driverFolder);
            foreach (var file in files)
            {
                logger.LogDebug(file);
            }
        }

        private static long ToUnixTime(DateTime dt)
        {
            return (long)(dt - UnixTimeEpoch).TotalMilliseconds;
        }
    }

    // Get worker input data from input stream
    internal class WorkerInputEnumerator : IEnumerator, IEnumerable
    {
        private static readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(WorkerInputEnumerator));

        private readonly Stream inputStream;
        private readonly string deserializedMode;

        // cache deserialized object read from input stream
        private object[] items = null;
        private int pos = 0;

        private readonly IFormatter formatter = new BinaryFormatter();
        private readonly Stopwatch watch = new Stopwatch();

        public WorkerInputEnumerator(Stream inputStream, string deserializedMode)
        {
            this.inputStream = inputStream;
            this.deserializedMode = deserializedMode;
        }

        public bool MoveNext()
        {
            watch.Start();
            bool hasNext;

            if ((items != null) && (pos < items.Length))
            {
                hasNext = true;
            }
            else
            {
                int messageLength = SerDe.ReadInt(inputStream);
                if (messageLength == (int)SpecialLengths.END_OF_DATA_SECTION)
                {
                    hasNext = false;
                    logger.LogDebug("END_OF_DATA_SECTION");
                }
                else if ((messageLength > 0) || (messageLength == (int)SpecialLengths.NULL))
                {
                    items = GetNext(messageLength);
                    Debug.Assert(items != null);
                    Debug.Assert(items.Any());
                    pos = 0;
                    hasNext = true;
                }
                else
                {
                    throw new Exception(string.Format("unexpected messageLength: {0}", messageLength));
                }
            }

            watch.Stop();
            return hasNext;
        }

        public object Current
        {
            get
            {
                int currPos = pos;
                pos++;
                return items[currPos];
            }
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public IEnumerator GetEnumerator()
        {
            return this;
        }

        public void LogStatistic()
        {
            logger.LogInfo(string.Format("total elapsed time: {0}", watch.ElapsedMilliseconds));
        }

        private object[] GetNext(int messageLength)
        {
            object[] result = null;
            switch ((SerializedMode)Enum.Parse(typeof(SerializedMode), deserializedMode))
            {
                case SerializedMode.String:
                    {
                        result = new object[1];
                        if (messageLength > 0)
                        {
                            byte[] buffer = SerDe.ReadBytes(inputStream, messageLength);
                            result[0] = SerDe.ToString(buffer);
                        }
                        else
                        {
                            result[0] = null;
                        }
                        break;
                    }

                case SerializedMode.Row:
                    {
                        Debug.Assert(messageLength > 0);
                        byte[] buffer = SerDe.ReadBytes(inputStream, messageLength);
                        var unpickledObjects = PythonSerDe.GetUnpickledObjects(buffer);
                        var rows = unpickledObjects.Select(item => (item as RowConstructor).GetRow()).ToList();
                        result = rows.Cast<object>().ToArray();
                        break;
                    }

                case SerializedMode.Pair:
                    {
                        byte[] pairKey = (messageLength > 0) ? SerDe.ReadBytes(inputStream, messageLength) : null;
                        byte[] pairValue = null;

                        int valueLength = SerDe.ReadInt(inputStream);
                        if (valueLength > 0)
                        {
                            pairValue = SerDe.ReadBytes(inputStream, valueLength);
                        }
                        else if (valueLength == (int)SpecialLengths.NULL)
                        {
                            pairValue = null;
                        }
                        else
                        {
                            throw new Exception(string.Format("unexpected valueLength: {0}", valueLength));
                        }

                        result = new object[1];
                        result[0] = new KeyValuePair<byte[], byte[]>(pairKey, pairValue);
                        break;
                    }

                case SerializedMode.None: //just read raw bytes
                    {
                        result = new object[1];
                        if (messageLength > 0)
                        {
                            result[0] = SerDe.ReadBytes(inputStream, messageLength);
                        }
                        else
                        {
                            result[0] = null;
                        }
                        break;
                    }

                case SerializedMode.Byte:
                default:
                    {
                        result = new object[1];
                        if (messageLength > 0)
                        {
                            byte[] buffer = SerDe.ReadBytes(inputStream, messageLength);
                            var ms = new MemoryStream(buffer);
                            result[0] = formatter.Deserialize(ms);
                        }
                        else
                        {
                            result[0] = null;
                        }

                        break;
                    }
            }

            return result;
        }
    }
}
