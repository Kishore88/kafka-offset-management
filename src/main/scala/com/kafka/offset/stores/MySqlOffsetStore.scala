package com.kafka.offset.stores

import java.sql.DriverManager
import java.util.Properties

import scala.collection.mutable.MutableList

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.OffsetRange

import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD

class MySqlOffsetsStore(jdbcUsername: String, jdbcPassword: String, jdbcHostname: String, jdbcPort: Int, jdbcDatabase: String) extends OffsetsStore {

  private val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
  private val connectionProperties = new Properties()
  connectionProperties.put("user", jdbcUsername)
  connectionProperties.put("password", jdbcPassword)
  private val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
  private val statement = connection.createStatement()

  println("Mysql connection created......")

  sys.addShutdownHook {
    println("closing mysql connection..........")
    connection.close()
  }

  /**
   * Read the previously saved offsets from Zookeeper
   * @param topic name for which stream to be created
   */
  override def readOffsets(topic: String): Option[Map[TopicAndPartition, Long]] = {

    val resultSet = statement.executeQuery("select * from topic_partitions_offset where topic = '" + (topic) + "'")
    var offsets: Map[TopicAndPartition, Long] = Map()

    while (resultSet.next()) {
      val my_topic = topic
      val my_partitions = resultSet.getString("partitions")

      val my_offsets = resultSet.getString("offset")
      println("Done reading offsets from Mysql.")
      offsets += (TopicAndPartition(my_topic, my_partitions.toInt) -> my_offsets.toLong)
    }

    if (offsets.isEmpty) {
      println("No offsets found in Mysql. ")
      None
    } else {
      Some(offsets)
    }

  }

  /**
   *  Save the offsets back to Mysql
   *
   *  @param datastream to read offsets and partitions
   *
   *  IMPORTANT: We're not saving the offset immediately but instead save the offset from the previous batch. This is
   *  because the extraction of the offsets has to be done at the beginning of the stream processing, before the real
   *  logic is applied. Instead, we want to save the offsets once we have successfully processed a batch, hence the
   *  workaround.
   */
  override def saveOffsets(dataStream: InputDStream[(String, String)]): Unit = {

    println("Saving offsets to Mysql")
    var offsetRanges = Array[OffsetRange]()
    dataStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      val ranges = new Array[String](offsetRanges.length)
      for (i <- 0 to offsetRanges.length - 1) {
        val topic = offsetRanges(i).topic
        val partition = offsetRanges(i).partition
        val offsets = offsetRanges(i).fromOffset
        println(s"${offsetRanges(i).topic} ${offsetRanges(i).partition} ${offsetRanges(i).fromOffset}")
        val resultSet = statement.executeQuery("select * from topic_partitions_offset where topic = '" + topic + "' and partitions = " + partition)
        if (resultSet.next()) {
          val updateQuery = ("UPDATE topic_partitions_offset SET "
            + "topic = '" + topic + "' , "
            + "partitions = " + partition + " , "
            + "offset = " + offsets + " "
            + "WHERE topic = '" + topic + "' "
            + "AND partitions = " + partition)

          statement.executeUpdate(updateQuery)
        } else {
          println("No Record found in mysql hence inserting.........")
          val x = statement.executeUpdate("INSERT INTO topic_partitions_offset (topic,partitions,offset)"
            + " VALUES (" + "'" + topic + "'" + "," + partition + "," + offsets + ")")
        }

      }
    }

  }
  
  /**
   *  delete the offsets
   *
   *  @param topic name
   */
  
  def deleteOffsets(topic: String):Unit = {
      println("deleting offsets to Mysql for topic : " + topic)
      val resultSet = statement.executeUpdate("delete from topic_partitions_offset where topic = '" + topic +"'")
        if (resultSet > 0) {
          println("Records deleted from mysql.........")
        } else {
          println("No record deleted.........")
        }
  }

  /**
   *  Save the offsets back to Mysql
   *
   *  @param topic name
   *  @param rdd to read partition and offsets
   */
  override def saveOffsets(topic: String, rdd: RDD[_]): Unit = {
  }

}