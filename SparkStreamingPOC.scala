package com.kafka.spark.streaming.consumer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.kafka.clients.KafkaClient
import org.apache.spark.streaming.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.mutable.ListBuffer

object SparkStreamingPOC {
  private var topic = ""
  private var broker = ""
  private var group = ""
  def main(args: Array[String]) {
    read(args)
    val conf = new SparkConf().setAppName("Spark Streaming Job")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("checkpoint")
    val topics=topic.split(",").toSet
    val kafkaParams = Map[String, String](
  "bootstrap.servers" -> "localhost:9092",
  "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
  "value.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer",
  "group.id" -> "Test",
  "auto.offset.reset" -> "latest"
)
    val dtream=KafkaUtils.createDirectStream(ssc, kafkaParams, topics).window(Seconds(10))
    var dStreamRDDList = new ListBuffer[RDD[String]]
    //val rdd=dtream.window(windowDuration)
    //dtream.foreachRDD(test =>{ dStreamRDDList += test}
    
    //)
    
        
    //)
  }
  def read(args: Array[String]) {

    broker = args(0)
    topic  = args(1)
    group  = args(2)
  }

}