package com.cloudera.spark_demo.streaming

import java.io.File
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import com.quantifind.sumac.{ArgMain, FieldArgs}

import scala.collection.JavaConverters._

object DataGen extends ArgMain[DataGenArgs] {
  override def main(args: DataGenArgs): Unit = {
    val start = System.currentTimeMillis()
    val end = TimeUnit.MINUTES.toMillis(args.maxMinutes) + start
    var idx = 0
    while (System.currentTimeMillis() < end) {
      val dest = new File(args.dir, idx + ".txt")
      val data: java.lang.Iterable[String] = Seq(idx.toString).asJava
      Files.write(dest.toPath(), data)
      Thread.sleep(TimeUnit.SECONDS.toMillis(args.secondsPerFile))
      idx += 1
    }
  }
}

class DataGenArgs extends FieldArgs {
  var dir: File = null
  var secondsPerFile = 10
  var maxMinutes = 60
}
