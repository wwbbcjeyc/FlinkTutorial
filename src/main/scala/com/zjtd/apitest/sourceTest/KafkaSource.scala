package com.zjtd.apitest.sourceTest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object KafkaSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.205.23:9092")
    properties.getProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val consumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]("ods-o_metis-user_i", new SimpleStringSchema(), properties)
    consumer.setStartFromTimestamp(1598426340000L)
    val inputStream: DataStream[String] = env.addSource(consumer)


    inputStream.print("inputStream:").setParallelism(1)

    env.execute()

  }
}
