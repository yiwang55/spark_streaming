package com.yiwang.ss

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateFulWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("StateFulWordCount").setMaster("local[2]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //
    ssc.checkpoint(".")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop001",9999, StorageLevel.MEMORY_AND_DISK_SER_2)

    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
