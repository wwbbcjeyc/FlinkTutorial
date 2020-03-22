package com.zjtd.apitest.sinkTest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zjtd.apitest.bean.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

//mysql jdbc sink
class MyJdbcSink extends RichSinkFunction[SensorReading]{

  //定义出sql连接、预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _


  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    //定义预编译语句
    conn.prepareStatement("INSERT INTO sensor_temp(sensor,temperature)VALUES(?,?)")
    conn.prepareStatement("UPDATE sensor_temp SET temperature = ? WHERE sensor = ?")
  }

  //每条数据来了后调用连接 执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
     //先执行更新语句
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()
    //如果没有更新数据，那么插入
    if(updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }


}
