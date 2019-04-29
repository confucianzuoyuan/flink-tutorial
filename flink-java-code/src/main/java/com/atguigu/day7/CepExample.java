package com.atguigu.day7;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class CepExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<LoginEvent> stream = env
                .fromElements(
                        new LoginEvent("user_1", "0.0.0.0", "fail", 2000L),
                        new LoginEvent("user_1", "0.0.0.1", "fail", 3000L),
                        new LoginEvent("user_1", "0.0.0.2", "fail", 4000L)
               )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent loginEvent, long l) {
                                return loginEvent.eventTime;
                            }
                        })
                )
                .keyBy(r -> r.userId);

        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .within(Time.seconds(5));


        PatternStream<LoginEvent> patternedStream = CEP.pattern(stream, pattern);

        patternedStream
                .select(new PatternSelectFunction<LoginEvent, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> select(Map<String, List<LoginEvent>> map) throws Exception {
                        LoginEvent first = map.get("first").iterator().next();
                        LoginEvent second = map.get("second").iterator().next();
                        LoginEvent third = map.get("third").iterator().next();
                        return Tuple4.of(first.userId, first.ipAddress, second.ipAddress, third.ipAddress);
                    }
                })
                .print();

        env.execute();
    }
}
