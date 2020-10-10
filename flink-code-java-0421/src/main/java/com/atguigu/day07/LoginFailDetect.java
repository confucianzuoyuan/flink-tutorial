package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class LoginFailDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<LoginEvent> stream = env
                .fromElements(
                        new LoginEvent("user_1", "0.0.0.0", "fail", 2000L),
                        new LoginEvent("user_1", "0.0.0.1", "fail", 3000L),
                        new LoginEvent("user_1", "0.0.0.2", "fail", 4000L),
                        new LoginEvent("user_1", "0.0.0.3", "fail", 4500L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                );

        // 定义恶意登录的模板
        Pattern<LoginEvent, LoginEvent> pattern1 = Pattern
                .<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                // 5s以内连续三次登录失败
                .within(Time.seconds(5));

        // 连续三次登录失败模板的另一种定义方法
        Pattern<LoginEvent, LoginEvent> pattern2 = Pattern
                .<LoginEvent>begin("first")
                .times(3)
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .within(Time.seconds(5));

        PatternStream<LoginEvent> patternStream1 = CEP.pattern(stream.keyBy(r -> r.userId), pattern1);

        PatternStream<LoginEvent> patternStream2 = CEP.pattern(stream.keyBy(r -> r.userId), pattern2);

        patternStream1
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> map) throws Exception {
                        LoginEvent first = map.get("first").iterator().next();
                        LoginEvent second = map.get("second").iterator().next();
                        LoginEvent thrid = map.get("third").iterator().next();

                        return first.ipAddr + "; " + second.ipAddr + "; " + thrid.ipAddr;
                    }
                })
                .print();

        patternStream2
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> map) throws Exception {
                        for (LoginEvent e : map.get("first")) {
                            System.out.println(e.ipAddr);
                        }
                        return "hello world";
                    }
                })
                .print();

        env.execute();
    }
}
