package com.atguigu.day6

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object WriteToMySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream.addSink(new MyJdbcSink)

    env.execute()
  }

  class MyJdbcSink extends RichSinkFunction[SensorReading] {
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/test",
        "root",
        "root"
      )

      insertStmt = conn.prepareStatement(
        "INSERT INTO temperatures (sensor, temp) VALUES (?, ?)"
      )

      updateStmt = conn.prepareStatement(
        "UPDATE temperatures SET temp = ? WHERE sensor = ?"
      )
    }

    // 执行sql
    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
      updateStmt.setDouble(1, value.temperature)
      updateStmt.setString(2, value.id)
      updateStmt.execute()

      // 如果没有更新成功，说明数据库中没有相应的数据
      if (updateStmt.getUpdateCount == 0) {
        insertStmt.setString(1, value.id)
        insertStmt.setDouble(2, value.temperature)
        insertStmt.execute()
      }
    }

    override def close(): Unit = {
      insertStmt.close()
      updateStmt.close()
      conn.close()
    }
  }
}