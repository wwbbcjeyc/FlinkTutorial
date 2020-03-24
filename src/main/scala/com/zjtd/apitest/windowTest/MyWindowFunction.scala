package com.zjtd.apitest.windowTest

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 自定义一个 window function
class MyWindowFunction() extends WindowFunction[SensorReading, String, Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[String]): Unit = {
    out.collect(key + " " + window.getStart)
  }
}
