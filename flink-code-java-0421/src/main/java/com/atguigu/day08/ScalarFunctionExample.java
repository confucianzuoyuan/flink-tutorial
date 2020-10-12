package com.atguigu.day08;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class ScalarFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        Table table = tEnv
                .fromDataStream(
                        stream,
                        $("id"),
                        $("timestamp").as("ts"),
                        $("temperature"),
                        $("pt").proctime());

        tEnv.createTemporaryView(
                "sensor",
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());

        tEnv.getConfig().addJobParameter("hashcode_factor", "31");

        // table api
        Table tableResult = tEnv.from("sensor").select(call(HashCodeFunction.class, $("id")));
//        tEnv.toAppendStream(tableResult, Row.class).print();

        // sql写法
        // 注册udf函数
        tEnv.createTemporarySystemFunction("hashCode", HashCodeFunction.class);

        Table sqlResult = tEnv.sqlQuery("SELECT id, hashCode(id) FROM sensor");
        tEnv.toAppendStream(sqlResult, Row.class).print();


        env.execute();
    }

    public static class HashCodeFunction extends ScalarFunction {
        private Integer factor = 0;

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            factor = Integer.parseInt(context.getJobParameter("hashcode_factor", "12"));
        }

        public Integer eval(String s) {
            return s.hashCode() * factor;
        }
    }
}
