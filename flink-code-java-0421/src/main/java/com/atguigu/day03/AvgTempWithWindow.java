package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class AvgTempWithWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .keyBy(r -> r.id)
                .timeWindow(Time.seconds(5))
                .process(new AvgTempPerWindow())
                .print();

        env.execute();
    }

    public static class AvgTempPerWindow extends ProcessWindowFunction<SensorReading, String, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<SensorReading> iterable, Collector<String> collector) throws Exception {
            Double sum = 0.0;
            Long count = 0L;

            for (SensorReading r : iterable) {
                sum += r.temperature;
                count += 1L;
            }

            collector.collect("传感器为：" + key + " 窗口结束时间为：" + new Timestamp(ctx.window().getEnd()) + " 的平均值是：" + sum / count);
        }
    }
}
