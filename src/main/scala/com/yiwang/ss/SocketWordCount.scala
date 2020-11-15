package com.yiwang.ss

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object SocketWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SocketWordCount").setMaster("local[2]")

    val context: StreamingContext = new StreamingContext(conf, Seconds(5))

    val lines: ReceiverInputDStream[String] = context.socketTextStream("hadoop001",9999, StorageLevel.MEMORY_AND_DISK_SER_2)

    val ds: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    ds.print()

    context.start()
    context.awaitTermination()
  }
}
