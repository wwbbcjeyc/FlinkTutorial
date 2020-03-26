/*
package com.zjtd.apitest.Stateback

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

// 自定义函数类，应用operator state
class MyProcessFunction extends ProcessFunction[SensorReading, String] with ListCheckpointed[Long]{
  private var myState: Long = _

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, String]#Context, out: Collector[String]): Unit = ???

  override def restoreState(state: util.List[Long]): Unit = ???

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Long] = ???
}
*/
