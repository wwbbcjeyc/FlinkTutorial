package com.zjtd.apitest.ProdessFunction

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.streaming.api.scala._

object SideOutputTest {
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

    // 利用process function得到侧输出流，做分流操作
    val highTempStream = dataStream
      .process( new SplitTemp(30.0) )

    // 获取侧输出流数据
    val lowTempStream = highTempStream.getSideOutput(new OutputTag[(String, Double, Long)]("low-temp"))

    highTempStream.print("high")
    lowTempStream.print("low")
    env.execute()
  }
}
