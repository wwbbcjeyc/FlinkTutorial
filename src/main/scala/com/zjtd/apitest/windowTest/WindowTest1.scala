package com.zjtd.apitest.windowTest

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest1 {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    //基本转换操作,转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
     // .assignAscendingTimestamps(_.timestamp * 1000)  //处理非乱序事件时使用
      //.assignTimestampsAndWatermarks(new MyPeriodicAssigner(1000L))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
      })

    //10秒内最小的温度
    val minTempPerWindowStream: DataStream[(String, Double)] = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    dataStream.print("input")
    minTempPerWindowStream.print("min temp")
      
    env.execute("window test")

  }

}
