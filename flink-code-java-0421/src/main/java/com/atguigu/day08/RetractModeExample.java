package com.atguigu.day08;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

// 当查询中有聚合操作时，我们使用RetractStream
public class RetractModeExample {
    public static void main(String[] args) throws Exception {
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置为使用流模式
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        // 创建表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        // DataStream -> 动态表
        Table table = tEnv.fromDataStream(
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());

        // table api
        Table tableResult = table
                .groupBy($("id"))
                .select($("id"), $("temperature").max());

//        tEnv.toRetractStream(tableResult, Row.class).print();

        // sql
        // 创建一张临时表
        tEnv.createTemporaryView(
                "sensor", // 临时表的名字
                stream, // 流
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());

//        tEnv.createTemporaryView("sensor", table);

        Table sqlResult = tEnv.sqlQuery("SELECT id, MAX(temperature) FROM sensor WHERE id = 'sensor_1' GROUP BY id");

        // 动态表 -> DataStream
        tEnv.toRetractStream(sqlResult, Row.class).print();

        env.execute();
    }
}
