package com.kafka.offset.stores

import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream

trait OffsetsStore {

  def readOffsets(topic: String,topic_tail:String): Option[Map[TopicAndPartition, Long]]

  def saveOffsets(topic: String, rdd: RDD[_]): Unit

  def saveOffsets(dataStream: InputDStream[(String, String)],topic_tail_string:String): Unit

}
