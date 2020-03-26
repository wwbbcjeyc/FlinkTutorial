package com.zjtd.apitest.ProdessFunction

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.streaming.api.scala._

object ProcessFuntionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // 基本转换操作，转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val warningStream = dataStream
      .keyBy("id")
      .process( new TempIncreWarning(5000L) )

    warningStream.print()
    env.execute()
  }
}
