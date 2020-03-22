/*
package com.zjtd.apitest.sinkTest

import java.sql.DriverManager

import com.zjtd.apitest.bean.SensorReading
import com.zjtd.apitest.sourceTest.SensorSource
import org.apache.flink.streaming.api.scala._

object JdbcSinkTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //读取数据
    val inputStream: DataStream[String] = env.readTextFile("/Users/joe/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt")

    //基本转换操作,转换成样例类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val dataStream2: DataStream[SensorReading] = env.addSource(new SensorSource)
    dataStream2.addSink(new MyJdbcSink())

    env.execute("jdbc sink test job")

  }
}
*/
