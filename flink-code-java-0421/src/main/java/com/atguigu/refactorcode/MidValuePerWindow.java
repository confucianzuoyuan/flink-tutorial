package com.atguigu.refactorcode;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MidValuePerWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new EventSource());

        stream
                .keyBy(e -> e.key)
                .timeWindow(Time.seconds(5))
                .process(new MidValue())
                .print();

        env.execute();
    }

    public static class MidValue extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            List<Long> list = new ArrayList<Long>();
            for (Event e : elements) {
                list.add(e.value);
            }
            list.sort(new Comparator<Long>() {
                @Override
                public int compare(Long v1, Long v2) {
                    return (int)(v1 - v2);
                }
            });
            out.collect("中位数是：" + list.get(list.size() / 2));
        }
    }

}













