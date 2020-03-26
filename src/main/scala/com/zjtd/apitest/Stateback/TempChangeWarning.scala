package com.zjtd.apitest.Stateback

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

// 自定义一个富函数，如果符合报警信息，输出（id, lastTemp, curTemp）
class TempChangeWarning(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  // 先声明状态，用来保存上一次的温度值
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 先读取状态
    val lastTemp = lastTempState.value()

    // 与当前最新的温度值求差值，比较判断
    val diff = (value.temperature - lastTemp).abs
    if( diff > threshold ){
      out.collect( (value.id, lastTemp, value.temperature) )
    }

    // 更新状态
    lastTempState.update(value.temperature)
  }
}
