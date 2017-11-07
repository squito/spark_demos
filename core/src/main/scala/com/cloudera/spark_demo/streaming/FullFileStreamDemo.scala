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
import java.nio.file.{Paths, Path, Files, FileVisitOption}
import java.util.Comparator
import java.util.concurrent.TimeUnit
import java.util.function.{Consumer => JConsumer, Function => JFunction}

import scala.language.implicitConversions

import com.quantifind.sumac.{FieldArgs, ArgMain}

object FullFileStreamDemo extends ArgMain[FullFileStreamDemoArgs] {
  override def main(args: FullFileStreamDemoArgs): Unit = {
    // simple way to run the data generator, and the streaming app in one jvm for demo purposes --
    // you wouldn't normally do it that way.
    val dir = new File("demo-data-dir")
    // https://stackoverflow.com/a/35989142/1442961
    println("deleting previous data")
    import Java8Helper._
    if (dir.exists()) {
      val jDeleteFunc: JConsumer[Path] = Files.delete(_: Path)
      Files.walk(dir.toPath, FileVisitOption.FOLLOW_LINKS)
        .sorted(Comparator.reverseOrder())
        .forEach(jDeleteFunc)
    }
    dir.mkdirs()
    println("starting data generator")
    val dataArgs = new DataGenArgs()
    dataArgs.dir = dir
    val dataGen = new Runnable {
      override def run(): Unit = DataGen.main(dataArgs)
    }
    val dataGenThread = new Thread(dataGen, "data-gen")
    dataGenThread.setDaemon(true)
    dataGenThread.start()


    // let a bit of data get generated
    println("waiting for some data, then starting spark streaming app")
    Thread.sleep(20)

    println("starting spark streaming app")
    val start = System.currentTimeMillis()
    val sparkArgs = new FileStreamDemoArgs()
    sparkArgs.dataDir = dir
    sparkArgs.checkpointDir = new File("checkpoint-dir")
    sparkArgs.sparkMaster = Some("local[2]")
    FileStreamDemo.main(sparkArgs)
    println("spark streaming has started")

    val end = TimeUnit.MINUTES.toMillis(5) + start
    while (System.currentTimeMillis() < end) {
      println("waiting on data gen and spark streaming ...")
      Thread.sleep(30 * 1000)
    }
    println("done, stopping everything")
    dataGenThread.interrupt()
    System.exit(0)
  }
}

class FullFileStreamDemoArgs extends FieldArgs {
}

object Java8Helper {

  implicit class JavaFunctionHelper[A, B](f: Function1[A, B]) {
    def toJavaFunction = new JFunction[A, B] {
      override def apply(a: A): B = f(a)
    }
  }

  class JavaConsumerHelper[A](f: Function1[A, Unit]) {
    def toConsumer = new JConsumer[A] {
      override def accept(a: A): Unit = f(a)
    }
  }

  // I can't get this to work without a *fully* implicit conversion
  implicit def toConsumerHelper[A](f: Function1[A, Unit]): JConsumer[A] =
    new JavaConsumerHelper(f).toConsumer
}
