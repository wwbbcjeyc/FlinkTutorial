package com.zjtd.apitest.sinkTest

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

//自定义RedisMapper
class MyRedisMapper extends RedisMapper[SensorReading] {
  // 保存数据到redis的命令定义，保存成一张hash表，HSET sensor_temp id temperature
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_tmp")
  }

  override def getKeyFromData(t: SensorReading): String = t.id

  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}
