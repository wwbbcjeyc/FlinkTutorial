package com.zjtd.apitest.ProdessFunction

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// 自定义Process Function，实现具体的分流功能
class SplitTemp(threshold: Double) extends ProcessFunction[SensorReading, SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 判断温度值，是否小于阈值，如果小于输出到侧输出流
    if( value.temperature < threshold ){
      ctx.output(new OutputTag[(String, Double, Long)]("low-temp"), (value.id, value.temperature, value.timestamp))
    } else {
      out.collect(value)
    }
  }
}
