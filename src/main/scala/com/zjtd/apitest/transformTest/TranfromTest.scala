package com.zjtd.apitest.transformTest

import com.zjtd.apitest.bean.SensorReading
import com.zjtd.apitest.customFunction.{MyFilter, MyFlatMap, MyMapper, MyRichMapper}
import org.apache.flink.streaming.api.scala._

object TranfromTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    //读取数据
    val inputStream: DataStream[String] = env.readTextFile("/Users/joe/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt")

    //1.基本转换炒作,转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    //2. keyBy 之后做聚合,获取到目前为止的温度最小值
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy(_.id)
      //.minBy("temperature")
      .reduce(
        (curState, newDate) =>
          SensorReading(curState.id, newDate.timestamp, newDate.temperature.min(curState.temperature))
      )
   // aggStream.print()

    //3.分流，按照温度值高低拆分
    val splitStream: SplitStream[SensorReading] = dataStream.split(
      sensorData => {
        if (sensorData.temperature > 30)
          Seq("highTemp")
        else
          Seq("lowTemp")
      }
    )
    val highTempStream: DataStream[SensorReading] = splitStream.select("highTemp")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("lowTemp")
    val allTempStream: DataStream[SensorReading] = splitStream.select("highTemp","lowTemp")

    //lowTempStream.print()
    //4.合流，connect 连接两条类型不同的流
    val waringStream: DataStream[(String, Double)] = highTempStream
      .map(data => (data.id, data.temperature))
    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = waringStream.connect(lowTempStream)
    val resultStream: DataStream[Product] = connectedStreams.map(
      waringData => (waringData._1, waringData._2, "high temp waring"),
      lowTempData => (lowTempData.id, "healthy")
    )
    //resultStream.print()

    //5. 合并两条流：Union
    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream, allTempStream)

    unionStream.print()

    //6.自定义函数类
    val transformStream1: DataStream[Int] = dataStream.map(new MyMapper)
    //transformStream1.print()
    val transformStream2: DataStream[SensorReading] = dataStream.filter(new MyFilter("sensor_1"))
    //transformStream2.print()





    env.execute()

  }
}
