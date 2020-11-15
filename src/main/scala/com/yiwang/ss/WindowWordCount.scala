package com.yiwang.ss

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WindowWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WindowWordCount").setMaster("local[2]")

    val scc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val lines: ReceiverInputDStream[String] = scc.socketTextStream("hadoop001",9999, StorageLevel.MEMORY_AND_DISK_SER_2)

    val ds: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(10), Seconds(5))

    ds.print()

    scc.start()
    scc.awaitTermination()
  }
}
