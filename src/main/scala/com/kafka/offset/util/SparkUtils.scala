package com.kafka.offset.util

import java.util.Properties

import scala.collection.immutable.Map
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s.DefaultFormats

import com.kafka.offset.stores.OffsetsStore

import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

/**
 * SparkUtils contains the common utilities for spark-scala
 */
class SparkUtils(properties: Properties) extends Serializable {

  implicit val formats = DefaultFormats

  val kafkaParams = Map[String, String]("metadata.broker.list" -> properties.getProperty("metadataBrokerList"),
    "group.id" -> properties.getProperty("group.id"), "auto.offset.reset" -> properties.getProperty("autoOffsetReset"))

  /**
   * create a spark context
   *
   *  @return spark context
   */
  def getSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName(properties.getProperty("appName"))
      .setMaster(properties.getProperty("master"))
      .set("spark.ui.port", properties.getProperty("spark.ui.port"))
      .set("spark.sql.shuffle.partitions", properties.getProperty("noOfPartitions"))
    new SparkContext(conf)
  }

    /**
   * create a spark context using spark conf
   *
   *  @return spark context
   */
  def getSparkContext(conf : SparkConf): SparkContext = {
    new SparkContext(conf)
  }
  
    /**
   * create a spark conf
   *
   *  @return spark conf
   */
  def getSparkConf(): SparkConf = {
   new SparkConf().setAppName(properties.getProperty("appName"))
      .setMaster(properties.getProperty("master"))
      .set("spark.ui.port", properties.getProperty("spark.ui.port"))
      .set("spark.sql.shuffle.partitions", properties.getProperty("noOfPartitions"))
  }
  
  /**
   * create a spark streaming context
   *
   *  @return spark streaming context
   */
  def createSparkStreamingContext(): StreamingContext = {
    val sc = getSparkContext
    new StreamingContext(sc, Seconds(properties.getProperty("batchDurationInSeconds").toInt))
  }

   /**
   * create a spark streaming context
   *
   *  @return spark streaming context
   */
  def createSparkStreamingContext(sc : SparkContext): StreamingContext = {
    new StreamingContext(sc, Seconds(properties.getProperty("batchDurationInSeconds").toInt))
  }
  
  /**
   * create a spark streaming context
   *
   *  @param ckPath checkpoint path for spark streaming context
   *
   *  @return spark streaming context
   */
  def getSparkStreamingContext(): StreamingContext = {
    val ssc = createSparkStreamingContext
    ssc
  }

  /*
   * creating kafkaStream with and without offsets
   * 
   *  @param ssc spark streaming context
   *  
   *  @param kafkaParams Map of kafka properties
   *  
   *  @param offsetsStore object to save and read offsets from zookeeper
   *  
   *  @param topic topic name from where to read
   *    
   */
  def createKafkaStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag](ssc: StreamingContext, kafkaParams: Map[String, String], offsetsStore: OffsetsStore, topic: String): InputDStream[(K, V)] = {

    val topics = Set(topic)

    val storedOffsets = offsetsStore.readOffsets(topic)
    val kafkaStream = storedOffsets match {
      case None =>
        // start from the latest offsets
        KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, topics)
      case Some(fromOffsets) =>
        // start from previously saved offsets
        val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, fromOffsets, messageHandler)
    }

    kafkaStream
  }

  
}
