package com.zjtd.apitest.customFunction

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

// 自定义一个富函数类
class MyRichMapper extends RichMapFunction[Double, Int]{
  var subTaskIndex = 0

  override def open(parameters: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
  }

  override def map(value: Double): Int = value.toInt

  override def close(): Unit = {
  }
}