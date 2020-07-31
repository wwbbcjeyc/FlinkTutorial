package com.zjtd.apitest.sinkTest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object ReadisSinkTest1 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取数据
    val inputStream: DataStream[String] = env.readTextFile("/Users/wangwenbo/Downloads/query-impala-630721.csv")

    val dataStream: DataStream[(String, String)] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      (dataArray(0) + "_" + dataArray(1), dataArray(2))
    })



    //向redis 写入数据
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("172.16.200.45")
      .setPort(6379)
      .setPassword("shX$t9TdiLjM4nJu")
      .setDatabase(0)
      .build();

    dataStream.addSink(new RedisSink[(String, String)](config,new MyRedisMappe1()))


    env.execute("redis sink test job")



  }
}

class MyRedisMappe1() extends RedisMapper[(String,String)]{
  override def getCommandDescription: RedisCommandDescription = {
     new RedisCommandDescription(RedisCommand.SADD)
  }

  override def getKeyFromData(data: (String, String)): String = data._1

  override def getValueFromData(data: (String, String)): String = data._2
}


