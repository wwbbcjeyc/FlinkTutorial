package com.zjtd.apitest.ProdessFunction

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

// 自定义 Process Function，用来定义定时器，检测温度连续上升
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String]{
  // 把上一次的温度值，保存成状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  // 把注册的定时器时间戳保存成状态，方便删除
  lazy val curTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-timer", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 先把状态取出来
    val lastTemp = lastTempState.value()
    val curTimerTs = curTimerState.value()

    lastTempState.update(value.temperature)

    // 判断：如果温度上升并且没有定时器，那么注册定时器
    if( value.temperature > lastTemp && curTimerTs == 0 ){
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      // 保存ts到状态
      curTimerState.update(ts)
    } else if( value.temperature < lastTemp ||  lastTemp == 0.0 ){
      // 如果温度值下降，直接删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      // 清空状态
      curTimerState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 定时器触发，报警
    out.collect("传感器"+ ctx.getCurrentKey + "温度连续5秒上升")
    // timer状态清空
    curTimerState.clear()
  }
}
