package com.zjtd.apitest.windowTest

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    //从调用时刻开始给env创建的每一个stream追加时间特性
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //周期性的生成watemark.默认是200毫秒
    //env.getConfig.setAutoWatermarkInterval(5000) 每隔5秒生成
    env.getConfig.setAutoWatermarkInterval(300L)



    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    //基本转换操作,转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      //.assignAscendingTimestamps(_.timestamp * 1000L)
      //.assignTimestampsAndWatermarks(new MyPeriodicAssigner(1000L))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = {
          //从数据中提取时间戳
          t.timestamp * 1000L
        }
      })

    //    dataStream.keyBy("id")
    //      .window( SlidingProcessingTimeWindows.of(Time.hours(1), Time.seconds(1), Time.hours(-8)) )
    //      .countWindow(10, 2)
    //      .timeWindow(Time.hours(1))
    //      .trigger()
    //      .evictor()
    //      .allowedLateness(Time.minutes(10))
    //      .sideOutputLateData()
    //      .sum(2)
    //      .getSideOutput()
    //      .apply( new MyWindowFunction() )


    // 输出每15秒每个传感器温度的最小值
    val minTempPerWinStream: DataStream[(String, Double)] = dataStream
      .map( data => (data.id, data.temperature) )
      .keyBy(_._1)    // 按照sensor id分组
      //      .timeWindow(Time.seconds(15))    // 15秒的滚动窗口
      .timeWindow(Time.seconds(15), Time.seconds(5))    // 15秒的滑动窗口，滑动步长5秒
      //      .reduce( (curMin, newData) => (curMin._1, curMin._2.min(newData._2)) )
      .reduce( new MyWindowFunction() )

    minTempPerWinStream.print()
    env.execute("window test job")



  }

}
