package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import com.atguigu.day03.util.HighLowTemp;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// AggregateFunction, ProcessWindowFunction
public class HighLowTempPerWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .keyBy(r -> r.id)
                .timeWindow(Time.seconds(5))
                .aggregate(new Agg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Tuple3<String, Double, Double>, HighLowTemp, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, Double, Double>> iterable, Collector<HighLowTemp> collector) throws Exception {
            Tuple3<String, Double, Double> iter = iterable.iterator().next();
            collector.collect(new HighLowTemp(key, iter.f1, iter.f2, context.window().getStart(), context.window().getEnd()));
        }
    }

    public static class Agg implements AggregateFunction<SensorReading, Tuple3<String, Double, Double>, Tuple3<String, Double, Double>> {
        @Override
        public Tuple3<String, Double, Double> createAccumulator() {
            return Tuple3.of("", Double.MAX_VALUE, Double.MIN_VALUE);
        }

        @Override
        public Tuple3<String, Double, Double> add(SensorReading r, Tuple3<String, Double, Double> agg) {
            return Tuple3.of(r.id, Math.min(r.temperature, agg.f1), Math.max(r.temperature, agg.f2));
        }

        @Override
        public Tuple3<String, Double, Double> getResult(Tuple3<String, Double, Double> agg) {
            return agg;
        }

        @Override
        public Tuple3<String, Double, Double> merge(Tuple3<String, Double, Double> stringDoubleDoubleTuple3, Tuple3<String, Double, Double> acc1) {
            return null;
        }
    }
}
