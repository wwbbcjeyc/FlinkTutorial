package com.zjtd.apitest.sourceTest

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

// 自定义 source function
class SensorSource() extends SourceFunction[SensorReading] {
  // 定义一个运行标志位
  var running: Boolean = true

  override def cancel(): Unit = running = false

  // 通过上下文ctx，连续不断地生成数据，发送到流里
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化一个随机数发生器
    val rand = new Random()
    // 随机生成10个传感器的温度值初始值
    var curTemp = 1.to(10).map(
      // 转换成(sensorId, temperature)二元组
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )

    // 无限循环，产生数据
    while (running) {
      // 每个传感器各自随机更新温度值
      val curTime = System.currentTimeMillis()
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 利用ctx发送所有传感器的数据
      curTemp.foreach(
        data => ctx.collect(SensorReading(data._1, curTime, data._2))
      )
      // 间隔1s
      Thread.sleep(1000)
    }
  }
}