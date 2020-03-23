package com.zjtd.apitest.customFunction

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class MyFlatMap extends RichFlatMapFunction[Int,(Int,Int)]{

  var subTaskIndex = 0

  override def open(parameters: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
    //以下可以做一些初始化工作，例如建立一个和HDFS的连接
  }


  override def flatMap(in: Int, out: Collector[(Int, Int)]): Unit = {
      if(in % 2 == subTaskIndex){
        out.collect((subTaskIndex,in))
      }

  }

  override def close(): Unit = {
    //以下做一些清理工作，例如断开和HDFS的连接
  }
}
