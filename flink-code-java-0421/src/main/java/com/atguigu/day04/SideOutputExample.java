package com.atguigu.day04;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        OutputTag<String> outputTag = new OutputTag<String>("output") {};
        OutputTag<String> outputTag1 = new OutputTag<String>("output1") {
        };

        SingleOutputStreamOperator<SensorReading> process = stream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                        if (sensorReading.temperature < 32.0) {
                            context.output(outputTag, "小于32度的温度值发送到侧输出流中！");
                        }
                        if (sensorReading.temperature > 100.0) {
                            context.output(outputTag1, "larger than 100.0");
                        }
                        collector.collect(sensorReading);
                    }
                });

        SingleOutputStreamOperator<SensorReading> process1 = stream
                .keyBy(r -> r.id)
                .process(new KeyedProcessFunction<String, SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                        if (sensorReading.temperature < 32.0) {
                            context.output(outputTag, "小于32度的温度值发送到侧输出流中！");
                        }
                        collector.collect(sensorReading);
                    }
                });

        process1.getSideOutput(outputTag).print();


        process.getSideOutput(outputTag).print();
        process.getSideOutput(outputTag1).print();

        env.execute();
    }
}
