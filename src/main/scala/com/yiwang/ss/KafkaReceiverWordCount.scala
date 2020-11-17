package com.yiwang.ss

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * spark streaming 对接kafka方式一
 */
object KafkaReceiverWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 4){
      System.err.println("Usage KafkaReceiverWordCount <zkQuorum> <groupId> <topics> <numThread>")
    }
    val Array(zkQuorum,groupId,topics,numThread) = args

    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val map = topics.split(",").map(s => (s, numThread.toInt)).toMap

    val kafkaStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, map)

    val result = kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
