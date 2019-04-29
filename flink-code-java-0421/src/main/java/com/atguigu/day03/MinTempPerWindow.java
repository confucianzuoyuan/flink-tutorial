package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

// 需求：每个滚动窗口中的最小温度值
public class MinTempPerWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

//        SingleOutputStreamOperator<Tuple2<String, Double>> mapStream = stream
//                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
//                    @Override
//                    public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
//                        return Tuple2.of(sensorReading.id, sensorReading.temperature);
//                    }
//                });

        // lambda表达式
        SingleOutputStreamOperator<Tuple2<String, Double>> mapStream = stream
                .map(r -> Tuple2.of(r.id, r.temperature))
                .returns(new TypeHint<Tuple2<String, Double>>() {});

        KeyedStream<Tuple2<String, Double>, String> keyedStream = mapStream.keyBy(r -> r.f0);

        WindowedStream<Tuple2<String, Double>, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5));

        SingleOutputStreamOperator<Tuple2<String, Double>> result = windowedStream.reduce(new ReduceFunction<Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> reduce(Tuple2<String, Double> t1, Tuple2<String, Double> t2) throws Exception {
                if (t1.f1 > t2.f1) {
                    return t2;
                } else {
                    return t1;
                }
            }
        });

        result.print();

        env.execute("min temp per window");
    }
}
