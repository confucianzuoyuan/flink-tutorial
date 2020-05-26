package com.atguigu.day7

import com.atguigu.day2.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FlinkTableExample {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表环境, 使用blink planner

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    tableEnv
      .connect(new FileSystem().path("/Users/yuanzuo/Desktop/flink-tutorial/FlinkSZ1128/src/main/resources/sensor.txt"))
      .withFormat(new Csv()) // 按照csv文件格式解析文件
      .withSchema( // 定义表结构
        new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")    // 创建临时表

    val sensorTable: Table = tableEnv.from("inputTable") // 将临时表inputTable赋值到sensorTable
    // 使用table api
    val resultTable: Table = sensorTable
      .select("id, temperature") // 查询`id`, `temperature` => (String, Double)
      .filter("id = 'sensor_1'") // 过滤

    resultTable
        .toAppendStream[(String, Double)] // 追加流
        .print()

    // 使用flink sql的方式查询
    val resultSqlTable: Table = tableEnv
      .sqlQuery("select id, temperature from inputTable where id ='sensor_1'")

    resultSqlTable
      .toAppendStream[(String, Double)] // 追加流
      .print()

    // 将DataStream转换成流表

    val stream = env.addSource(new SensorSource)

    // 将流转换成了动态表
    val table = tableEnv.fromDataStream(stream, 'id, 'timestamp as 'ts, 'temperature as 'temp)
    table
      .select('id, 'temp) // 生成了另一张动态表
      // 将表的每一行转换成`(String, Double)`元组类型
      .toAppendStream[(String, Double)] // 将动态表转换成了流
      .print()

    // 时间属性
    // 定义处理时间
    val tableProcessingTime = tableEnv
        // `'pt.proctime`：添加处理时间，字段名是`'pt`
        .fromDataStream(stream, 'id, 'timestamp as 'ts, 'temperature as 'temp, 'pt.proctime)

    // 定义事件时间
    val tableEventTime = tableEnv
        // 将timestamp指定为事件时间，并命名为ts
        .fromDataStream(stream, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)

    env.execute()
  }
}