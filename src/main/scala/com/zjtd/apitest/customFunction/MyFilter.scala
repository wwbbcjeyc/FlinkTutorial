package com.zjtd.apitest.customFunction

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.api.common.functions.FilterFunction

//自定义一个FilterFunction,筛选出以sensor_1开头的数据
class MyFilter(Keyword: String) extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith(Keyword)
  }
}
