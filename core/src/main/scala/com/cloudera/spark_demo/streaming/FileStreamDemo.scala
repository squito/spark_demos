/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.spark_demo.streaming

import java.io.File

import com.quantifind.sumac.validation.Required
import com.quantifind.sumac.{FieldArgs, ArgMain}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

object FileStreamDemo extends ArgMain[FileStreamDemoArgs] {
  override def main(args: FileStreamDemoArgs): Unit = {
    val conf = new SparkConf()
      .setAppName("FileStreamDemo")
    // because of cdh-snappy on a mac :(
    conf.set("spark.io.compression.codec", "lz4")
    args.sparkMaster.foreach { m => conf.setMaster(m) }
    val ssc = new StreamingContext(conf, Seconds(args.batchSeconds))
    ssc.checkpoint(args.checkpointDir.getAbsolutePath)

    val tempStream = ssc.textFileStream(args.dataDir.getAbsolutePath).map { x => (x.toInt % 10, x.toInt)}.
      reduceByKey { _ + _}
    // TODO how do you set the name?
    tempStream.cache()
    tempStream.countByWindow(
        Seconds(args.batchSeconds * args.windowMultiple),
        Seconds(args.batchSeconds * args.slideMultiple)
      ).
      print()
    ssc.start()
    // should main exit here?
  }
}

class FileStreamDemoArgs extends FieldArgs {
  @Required
  var dataDir: File = null
  @Required
  var checkpointDir: File = null
  var sparkMaster: Option[String] = None
  var batchSeconds = 5
  var slideMultiple = 2
  var windowMultiple = 5
}
