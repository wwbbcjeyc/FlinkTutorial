package com.zjtd.apitest.windowTest

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

// 自定义可间断地生成watermark的 assiger
class MyPuntuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading]{

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if(lastElement.id == "sensor_1"){
      new Watermark(extractedTimestamp)
    } else{
      null
    }
  }
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}
