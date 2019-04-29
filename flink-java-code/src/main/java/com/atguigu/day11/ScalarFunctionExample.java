package com.atguigu.day11;

import com.atguigu.day2.SensorReading;
import com.atguigu.day2.SensorSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.getConfig().addJobParameter("hashcode_factor", "31");

        tableEnv.createTemporaryView("sensor", stream);

        // table api
        Table tableResult = tableEnv
                .from("sensor")
                .select(call(HashCodeFunction.class, $("id")));

        tableEnv.toAppendStream(tableResult, TypeInformation.of(Row.class)).print();

        // sql
        tableEnv.createTemporarySystemFunction("hashCode", HashCodeFunction.class);

        Table sqlResult = tableEnv
                .sqlQuery("SELECT id, hashCode(id) FROM sensor");

        tableEnv.toAppendStream(sqlResult, TypeInformation.of(Row.class)).print();

        env.execute();
    }

    public static class HashCodeFunction extends ScalarFunction {
        private int factor = 0;

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
