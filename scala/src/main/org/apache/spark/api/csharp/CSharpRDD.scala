// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package org.apache.spark.api.csharp

import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import org.apache.spark.api.python.{PythonBroadcast, PythonRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkContext}

/**
 * RDD used for forking an external C# process and pipe in & out the data
 * between JVM and CLR. Since PythonRDD already has the required implementation
 * it just extends from it without overriding any behavior for now
 */
class CSharpRDD(
                                @transient parent: RDD[_],
                                command: Array[Byte],
                                envVars: JMap[String, String],
                                cSharpIncludes: JList[String],
                                preservePartitoning: Boolean,
                                cSharpExec: String,
                                cSharpVer: String,
                                broadcastVars: JList[Broadcast[PythonBroadcast]],
                                accumulator: Accumulator[JList[Array[Byte]]])
  extends PythonRDD (parent, command, envVars, cSharpIncludes, preservePartitoning, cSharpExec, cSharpVer, broadcastVars, accumulator) {

}

object CSharpRDD {
  def createRDDFromArray(sc: SparkContext, arr: Array[Array[Byte]], numSlices: Int): RDD[Array[Byte]] = {
    sc.parallelize(arr, numSlices)
  }
}