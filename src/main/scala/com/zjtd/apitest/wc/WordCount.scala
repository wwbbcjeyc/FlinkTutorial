package com.zjtd.apitest.wc

import org.apache.flink.api.scala._

//批处理
object WordCount {

  def main(args: Array[String]): Unit = {
    //1.创建一个批处理环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2.从文件读取数据
    val inputPath: String = "/Users/joe/IdeaProjects/FlinkTutorial/src/main/resources/hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)
    //3.对数据进行处理,计算word count:(word,count)
    val resultDataSet: AggregateDataSet[(String, Int)] = inputDataSet
      .flatMap(x => x.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //打印输出
    resultDataSet.print()



  }

}
