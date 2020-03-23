package com.zjtd.apitest.customFunction

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

//自定义一个MapFunction
class MyMapper extends MapFunction[SensorReading,Int]{
  override def map(value: SensorReading): Int = {
    value.temperature.toInt
  }
}
