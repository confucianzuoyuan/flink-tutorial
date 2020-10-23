package com.atguigu.day03;

import com.atguigu.refactorcode.Event;
import com.atguigu.refactorcode.EventSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class AvgValueWithWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new EventSource());

        stream
                .keyBy(e -> e.key)
                .timeWindow(Time.seconds(5))
                .process(new AvgValuePerWindow())
                .print();

        env.execute();
    }

    public static class AvgValuePerWindow extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Event> iterable, Collector<String> collector) throws Exception {
            Double sum = 0.0;
            long count = 0L;

            for (Event e : iterable) {
                sum += e.value;
                count += 1L;
            }

            collector.collect("key为：" + key + " 窗口结束时间为：" + new Timestamp(ctx.window().getEnd()) + " 的平均值是：" + sum / count);
        }
    }
}
