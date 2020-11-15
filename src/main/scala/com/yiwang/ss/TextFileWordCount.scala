package com.yiwang.ss

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TextFileWordCount {
  def main(args: Array[String]): Unit = {
    //读文件不需要Receiver 所以本地模式可以设置一个线程
    val conf = new SparkConf().setAppName("TextFileWordCount").setMaster("local")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.textFileStream("D:\\bigdata\\codefiles\\SparkStreaming\\")

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
