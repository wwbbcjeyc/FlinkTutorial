package com.zjtd.apitest.sourceTest

import java.util.Properties

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //Source 从集合中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_2", 1547718201, 15.4),
      SensorReading("sensor_3", 1547718202, 6.7),
      SensorReading("sensor_4", 1547718205, 38.1)
    ))

    //从元素
    //val stream2: DataStream[Any] = env.fromElements(1, 0.4, "hello")

    //从文件
    //val stream3: DataStream[String] = env.readTextFile("/Users/joe/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt")

    //从kafka
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.getProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream4: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    //自定义
    val stream5: DataStream[SensorReading] = env.addSource(new SensorSource)


    stream5.print("Stream5:").setParallelism(1)

    env.execute()

  }

}
