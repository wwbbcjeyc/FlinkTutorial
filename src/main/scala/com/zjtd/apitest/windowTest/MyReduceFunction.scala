package com.zjtd.apitest.windowTest

import org.apache.flink.api.common.functions.ReduceFunction

// 自定义一个增量聚合函数
class MyReduceFunction() extends ReduceFunction[(String, Double)]{
  override def reduce(value1: (String, Double), value2: (String, Double)): (String, Double) = {
    ( value1._1, value1._2.min(value2._2) )
  }
}
