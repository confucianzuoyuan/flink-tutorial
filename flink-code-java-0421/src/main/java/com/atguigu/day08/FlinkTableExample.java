package com.atguigu.day08;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class FlinkTableExample {
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

        // 流 -> 表
        Table table = tEnv.fromDataStream(
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());

        // table api
        Table tableResult = table
                // 10s的滚动窗口，使用pt字段，也就是处理时间，给窗口取一个别名win
                .window(Tumble.over(lit(10).seconds()).on($("pt")).as("win"))
                // .keyBy(id).timeWindow(10s)
                .groupBy($("id"), $("win"))
                .select($("id"), $("id").count());

        tEnv.toRetractStream(tableResult, Row.class).print();

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

        Table sqlResult = tEnv.sqlQuery("SELECT id, COUNT(id) FROM sensor WHERE id = 'sensor_1' GROUP BY id, TUMBLE(pt, INTERVAL '10' SECOND)");

        tEnv.toRetractStream(sqlResult, Row.class).print();

        env.execute();
    }
}
