package com.atguigu.day08;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class AggregateFunctionExample {
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

        // 流 -> 表
        Table table = tEnv.fromDataStream(
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());

        tEnv.createTemporaryView("sensor", stream);

        tEnv.registerFunction("avgTemp", new AvgTemp());
        Table sqlResult = tEnv.sqlQuery("SELECT id, avgTemp(temperature) FROM sensor GROUP BY id");
        tEnv.toRetractStream(sqlResult, Row.class).print();

        env.execute();
    }

    public static class AvgTempAcc {
        public Double sum = 0.0;
        public Integer count = 0;

        public AvgTempAcc() {
        }

        public AvgTempAcc(Double sum, Integer count) {
            this.sum = sum;
            this.count = count;
        }
    }

    public static class AvgTemp extends AggregateFunction<Double, AvgTempAcc> {
        @Override
        public AvgTempAcc createAccumulator() {
            return new AvgTempAcc();
        }

        public void accumulate(AvgTempAcc acc, Double temp) {
            acc.sum += temp;
            acc.count += 1;
        }

        @Override
        public Double getValue(AvgTempAcc acc) {
            return acc.sum / acc.count;
        }
    }
}
