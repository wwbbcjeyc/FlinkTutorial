package com.zjtd.apitest.sinkTest

import java.util.Properties

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.getProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //读取数据
    //val inputStream: DataStream[String] = env.readTextFile("/Users/joe/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt")
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))



    //转换成样例类类型
    val dataStream: DataStream[String] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        dataArray
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble).toString
      })

    //输出到kafka
    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092","sinkTest",new SimpleStringSchema()))

    env.execute()
  }
}
