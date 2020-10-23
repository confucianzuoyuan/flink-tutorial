package com.atguigu.refactorcode;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AvgValuePerWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new EventSource());

        stream
                .keyBy(r -> r.key)
                .timeWindow(Time.seconds(5))
                .aggregate(new AvgValue())
                .print();

        env.execute();
    }
    
    public static class AvgValue implements AggregateFunction<Event, Tuple3<String, Long, Long>, Tuple2<String, Long>> {
        @Override
        public Tuple3<String, Long, Long> createAccumulator() {
            return Tuple3.of("", 0L, 0L);
        }

        @Override
        public Tuple3<String, Long, Long> add(Event e, Tuple3<String, Long, Long> acc) {
            return Tuple3.of(e.key, acc.f1 + e.value, acc.f2 + 1L);
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple3<String, Long, Long> acc) {
            return Tuple2.of(acc.f0, acc.f1 / acc.f2);
        }

        @Override
        public Tuple3<String, Long, Long> merge(Tuple3<String, Long, Long> acc1, Tuple3<String, Long, Long> acc2) {
            return null;
        }
    }
}
