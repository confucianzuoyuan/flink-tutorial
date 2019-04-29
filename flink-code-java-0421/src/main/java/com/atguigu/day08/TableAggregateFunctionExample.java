package com.atguigu.day08;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

public class TableAggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置为使用流模式
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        // 创建表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        tEnv.registerFunction("top2", new Top2Temp());

        // 流 -> 表
        Table table = tEnv.fromDataStream(
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());

        Table tableResult = table
                .groupBy($("id"))
                .flatAggregate("top2(temperature) as (temp, rank)")
                .select($("id"), $("temp"), $("rank"));

        tEnv.toRetractStream(tableResult, Row.class).print();

        env.execute();
    }
    public static class Top2TempAcc {
        public Double highestTemp = Double.MIN_VALUE;
        public Double secondHighestTemp = Double.MIN_VALUE;

        public Top2TempAcc(Double highestTemp, Double secondHighestTemp) {
            this.highestTemp = highestTemp;
            this.secondHighestTemp = secondHighestTemp;
        }

        public Top2TempAcc() {
        }
    }

    public static class Top2Temp extends TableAggregateFunction<Tuple2<Double, Integer>, Top2TempAcc> {
        @Override
        public Top2TempAcc createAccumulator() {
            return new Top2TempAcc();
        }

        public void accumulate(Top2TempAcc acc, Double temp) {
            if (temp > acc.highestTemp) {
                acc.secondHighestTemp = acc.highestTemp;
                acc.highestTemp = temp;
            } else if (temp > acc.secondHighestTemp) {
                acc.secondHighestTemp = temp;
            }
        }

        public void emitValue(Top2TempAcc acc, Collector<Tuple2<Double, Integer>> out) {
            out.collect(Tuple2.of(acc.highestTemp, 1));
            out.collect(Tuple2.of(acc.secondHighestTemp, 2));
        }
    }
}
