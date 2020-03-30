package com.zjtd.apitest.Stateback

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

object StateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 状态后端配置
    //    env.setStateBackend(new FsStateBackend(""))
    //    env.setStateBackend( new RocksDBStateBackend("") )


    // checkpoint配置
    env.enableCheckpointing(10000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(20000L)
    //    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

    // 重启策略的配置
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
    //    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // 基本转换操作，转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 检测传感器温度，如果连续两次的温度差值超过10度，输出报警
    val warningStream = dataStream
      .keyBy("id")
      //      .flatMap( new TempChangeWarning(10.0) )
      .flatMapWithState[(String, Double, Double), Double]({
            //如果没有状态，说明数据没来过，那么就将当前数据温度存入状态
        case (inputData: SensorReading, None) => ( List.empty, Some(inputData.temperature) )
            //如果有状态，就应该与上次温度做差，大于阀值输出报警
        case (inputData: SensorReading, lastTemp: Some[Double]) => {
          val diff = (inputData.temperature - lastTemp.get).abs
          if( diff > 10.0 ){
            (List((inputData.id, lastTemp.get, inputData.temperature)), Some(inputData.temperature))
          } else
            (List.empty, Some(inputData.temperature))
        }
      })

    warningStream.print()
    env.execute()
  }
}
