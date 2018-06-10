package com.kafka.offset

import java.io.FileInputStream
import java.util.Properties

import com.kafka.offset.stores.MySqlOffsetsStore
import com.kafka.offset.util.SparkUtils

import _root_.kafka.serializer.StringDecoder

object Parser {

  val properties = new Properties()

  /*
	 * main function
	 */
  def main(args: Array[String]): Unit = {

    try {
      properties.load(new FileInputStream(args(0)))

      val kafkaParams = Map[String, String](
        "metadata.broker.list" -> properties.getProperty("metadataBrokerList"), "fetch.message.max.bytes" -> properties.getProperty("fetch.message.max.bytes"),
        "group.id" -> properties.getProperty("group.id"), "auto.offset.reset" -> properties.getProperty("autoOffsetReset"))
      val su = new SparkUtils(properties)

      val conf = su.getSparkConf();
      conf.set("spark.streaming.unpersist", "true")
      conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

      val sc = su.getSparkContext(conf)
      val topic = properties.getProperty("inletKafkaTopic")
      val ssc = su.createSparkStreamingContext(sc)

      // environment variable
      val databaseName = properties.getProperty("jdbcDatabase")
      val offsetStore = new MySqlOffsetsStore(properties.getProperty("jdbcUsername"), properties.getProperty("jdbcPassword"), properties.getProperty("jdbcHostname"), properties.getProperty("jdbcPort").toInt, databaseName)
      val dataStream = su.createKafkaStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, offsetStore, topic, "")
      val propertiesBroadcast = dataStream.context.sparkContext.broadcast(properties)

      val mappedDataStream = dataStream.map(_._2)
      mappedDataStream.foreachRDD { rdd =>
        try {
          if (!rdd.isEmpty()) {
            //TODO
            // logic
          }
        } catch {
          case all: Exception =>
            if (all.getCause != null && all.getCause.toString().equalsIgnoreCase("kafka.common.OffsetOutOfRangeException")) {
              // delete the offset value from MySql
              offsetStore.deleteOffsets(topic)
            } else {
              all.printStackTrace()
            }
        }
      }
      // save the offsets
      offsetStore.saveOffsets(dataStream)

      ssc.start()
      ssc.awaitTermination()
    } catch {
      case all: Exception =>
        all.printStackTrace()

    }
  }

}