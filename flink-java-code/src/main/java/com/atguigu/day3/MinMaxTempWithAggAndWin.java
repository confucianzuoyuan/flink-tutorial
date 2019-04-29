package com.atguigu.day3;

import com.atguigu.day2.SensorReading;
import com.atguigu.day2.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MinMaxTempWithAggAndWin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .keyBy(r -> r.id)
                .timeWindow(Time.seconds(5))
                .aggregate(new Agg(), new Win())
                .print();

        env.execute();
    }

    public static class Win extends ProcessWindowFunction<Tuple2<Double, Double>, MinMaxTemp, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Tuple2<Double, Double>> elements, Collector<MinMaxTemp> out) throws Exception {
            Tuple2<Double, Double> minMax = elements.iterator().next();
            out.collect(new MinMaxTemp(s, minMax.f0, minMax.f1, ((Long)context.window().getEnd()).toString()));
        }
    }

    public static class Agg implements AggregateFunction<SensorReading, Tuple2<Double, Double>, Tuple2<Double, Double>> {
        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return Tuple2.of(Double.MAX_VALUE, Double.MIN_VALUE);
        }

        @Override
        public Tuple2<Double, Double> add(SensorReading value, Tuple2<Double, Double> accumulator) {
            if (value.temperature > accumulator.f1) {
                accumulator.f1 = value.temperature;
            }
            if (value.temperature < accumulator.f0) {
                accumulator.f0  = value.temperature;
            }
            return accumulator;
        }

        @Override
        public Tuple2<Double, Double> getResult(Tuple2<Double, Double> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<Double, Double> merge(Tuple2<Double, Double> a, Tuple2<Double, Double> b) {
            return null;
        }
    }
}
