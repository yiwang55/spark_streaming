package com.yiwang.ss

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
流数据格式
20180506,zs
20180506,ls
20180506,ww
 */
object NetWorkBlackList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetWorkBlackList").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))

    //模拟数据库存在黑名单list
    val list = List("ls","zs")

    //构建rdd 用于join
    val blackRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(list).map(e => {
      (e, true)
    })

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop001",9999)

    //转换成("zs","20180506,zs")格式
    val tulpeDStream: DStream[(String, String)] = lines.map(line => (line.split(",")(1), line))

    val clickLog: DStream[String] = tulpeDStream.transform(rdd => {
      rdd.leftOuterJoin(blackRDD).filter(e =>
        e._2._2.getOrElse(false) != true
      ).map(x => x._2._1)
    })

    clickLog.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
