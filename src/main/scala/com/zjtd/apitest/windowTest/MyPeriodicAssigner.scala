package com.zjtd.apitest.windowTest

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

//自定义一个周期性的时间戳抽取:
class MyPeriodicAssigner(bound: Long) extends AssignerWithPeriodicWatermarks[SensorReading]{
  //定义当前最大时间戳
  var maxTs: Long = Long.MinValue //观察到的最大时间戳

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000L)
    element.timestamp
  }
}
