package com.zjtd.apitest.sinkTest

import java.util

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object EsSinkTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("/Users/joe/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt")

    // 基本转换操作，转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
    //定义一个HttpHost的List
    val httpHosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost",9200))

    //创建一个esSink的Builder
    val esSinkBuilder: ElasticsearchSink.Builder[SensorReading] = new ElasticsearchSink.Builder[SensorReading](httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + t)
          //从数据中提取信息，包装成index request,通过requestIndexer 发送出去
          //1.定义data source
          val dataSource: util.HashMap[String, String] = new util.HashMap[String, String]()
          dataSource.put("sensor_id", t.id)
          dataSource.put("temperature", t.temperature.toString)
          dataSource.put("ts", t.timestamp.toString)
          //2.创建 index request
          val indexRequest: IndexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(dataSource)
          //3.用requestIndexer 发送http 请求
          requestIndexer.add(indexRequest)
          println("save successfully")
        }
      })

    dataStream.addSink(esSinkBuilder.build())
    env.execute("es sink test job")


  }

}
