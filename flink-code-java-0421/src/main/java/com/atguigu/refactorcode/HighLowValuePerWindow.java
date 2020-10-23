package com.atguigu.refactorcode;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HighLowValuePerWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new EventSource());

        stream
                .keyBy(e -> e.key)
                .timeWindow(Time.seconds(5))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class HighLowValue {
        public String key;
        public Long minValue;
        public Long maxValue;
        public Long windowStart;
        public Long windowEnd;

        public HighLowValue() {
        }

        public HighLowValue(String key, Long minValue, Long maxValue, Long windowStart, Long windowEnd) {
            this.key = key;
            this.maxValue = maxValue;
            this.minValue = minValue;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "HighLowValue{" +
                    "key='" + key + '\'' +
                    ", maxValue=" + maxValue +
                    ", minValue=" + minValue +
                    ", windowStart=" + windowStart +
                    ", windowEnd=" + windowEnd +
                    '}';
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Tuple3<String, Long, Long>, HighLowValue, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, Long, Long>> iterable, Collector<HighLowValue> collector) throws Exception {
            Tuple3<String, Long, Long> iter = iterable.iterator().next();
            collector.collect(new HighLowValue(key, iter.f1, iter.f2, context.window().getStart(), context.window().getEnd()));
        }
    }

    public static class CountAgg implements AggregateFunction<Event, Tuple3<String, Long, Long>, Tuple3<String, Long, Long>> {
        @Override
        public Tuple3<String, Long, Long> createAccumulator() {
            return Tuple3.of("", Long.MAX_VALUE, Long.MIN_VALUE);
        }

        @Override
        public Tuple3<String, Long, Long> add(Event e, Tuple3<String, Long, Long> agg) {
            return Tuple3.of(e.key, Math.min(e.value, agg.f1), Math.max(e.value, agg.f2));
        }

        @Override
        public Tuple3<String, Long, Long> getResult(Tuple3<String, Long, Long> agg) {
            return agg;
        }

        @Override
        public Tuple3<String, Long, Long> merge(Tuple3<String, Long, Long> acc1, Tuple3<String, Long, Long> acc2) {
            return null;
        }
    }
}
