package com.zjtd.apitest.wc


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {

  def main(args: Array[String]): Unit = {

    //1.创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // env.setParallelism(1)
    // env.disableOperatorChaining()
    //2 从socket 文本流中读取数据
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    val inputStream: DataStream[String] = env.socketTextStream(host, port)
    //3.对数据进行处理，计算word count
    val resultStream: DataStream[(String, Int)] = inputStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty).startNewChain()
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //4.打印输出
    resultStream.print()
    //启动任务
    env.execute()

  }

}
