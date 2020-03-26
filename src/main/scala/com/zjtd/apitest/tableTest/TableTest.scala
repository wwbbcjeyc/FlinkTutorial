package com.zjtd.apitest.tableTest

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._

object TableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据
    val inputStream: DataStream[String] = env.readTextFile("/Users/joe/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 先创建一个table执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 基于流创建一个表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 定义转换操作
    val resultTable: Table = dataTable
      .select("id, temperature")
      .filter("id = 'sensor_1'")

    // 结果表转换成流输出
    val resultStream: DataStream[(String, Double)] = resultTable.toAppendStream[(String, Double)]

    resultStream.print()
    env.execute("table api test")
  }
}
