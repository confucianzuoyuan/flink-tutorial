package com.atguigu.day3;

import com.atguigu.day2.SensorReading;
import com.atguigu.day2.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MinMaxTempPerWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));

        stream
                .keyBy(r -> r.id)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<SensorReading> elements, Collector<MinMaxTemp> out) throws Exception {
                        double min = Double.MAX_VALUE;
                        double max = Double.MIN_VALUE;
                        for (SensorReading element : elements) {
                            double temp = element.temperature;
                            if (temp >= max) {
                                max = temp;
                            }
                            if (temp < min) {
                                min = temp;
                            }
                        }
                        out.collect(new MinMaxTemp(s, min, max, ((Long)context.window().getEnd()).toString()));
                    }
                })
                .print();

        env.execute();
    }
}
