package com.atguigu.day08;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class EventtimeWindowConcept {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        SingleOutputStreamOperator<SensorReading> stream = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                                    @Override
                                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );


        Table table = tEnv
                .fromDataStream(
                        stream,
                        $("id"),
                        $("timestamp").rowtime().as("ts"),
                        $("temperature"));

//        table.window(Tumble.over(lit(10).seconds()).on($("ts")).as("win"))
//        table.window(Slide.over(lit(10).seconds()).every(lit(5).seconds()).on($("ts")).as("win"))

        tEnv.createTemporaryView(
                "sensor",
                stream,
                $("id"),
                $("timestamp").rowtime().as("ts"),
                $("temperature"));

        // 注意如何获取窗口的开始时间和结束时间
        Table sqlTumbleWindowResult = tEnv.sqlQuery("SELECT id, count(id), TUMBLE_START(ts, INTERVAL '10' SECOND), TUMBLE_END(ts, INTERVAL '10' SECOND) FROM sensor WHERE id = 'sensor_1' GROUP BY id, TUMBLE(ts, INTERVAL '10' SECOND)");
//        tEnv.toRetractStream(sqlTumbleWindowResult, Row.class).print();

        // 滑动窗口HOP(时间戳的字段，滑动距离，窗口长度)
        Table sqlSlideWindowResult = tEnv.sqlQuery("SELECT id, count(id), HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND), HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) FROM sensor WHERE id = 'sensor_1' GROUP BY id, HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)");
        tEnv.toRetractStream(sqlSlideWindowResult, Row.class).print();

        env.execute();
    }
}
