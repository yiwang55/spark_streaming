package com.yiwang.ss

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WordCountToMySQL {
  /**
   * 获取数据库连接
   */
  def getConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://hadoop001:3306/imooc_project?user=root&password=root&useUnicode=true&characterEncoding=UTF-8")
  }

  /**
   * 释放数据库连接等资源
   * @param connection
   * @param pstmt
   */
  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCountToMySQL").setMaster("local[2]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop001",9999)

    val ds: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    ds.foreachRDD(rdd =>{
      rdd.foreachPartition(partition => {
        if (partition.size>0){
        val connection = getConnection()
          print(connection)
        partition.foreach(line => {
          val sql = "insert into wordcount(word, wordcount) values('"+ line._1 + "'," + line._2+ ")"
          println(sql)
          connection.createStatement().execute(sql)
        })
          release(connection, null)
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
