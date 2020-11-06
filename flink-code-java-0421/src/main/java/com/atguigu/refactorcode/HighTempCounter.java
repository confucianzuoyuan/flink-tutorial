package com.atguigu.refactorcode;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class HighTempCounter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);
        SingleOutputStreamOperator<SensorReading> stream = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                            @Override
                            public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream
                .keyBy(r -> r.id)
                .flatMap(new HighTempCounterHelper(0.0))
                .print();
        env.execute();
    }

    public static class HighTempCounterHelper implements FlatMapFunction<SensorReading, Tuple3<String, Long, Long>>, CheckpointedFunction {

        // 在本地用于存储算子实例高温数目的变量
        private Long opHighTempCnt = 0L;
        private ValueState<Long> keyedCntState;
        private ListState<Long> opCntState;
        private Double threshold;

        public HighTempCounterHelper(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            if (value.temperature > this.threshold) {
                opHighTempCnt += 1;
                Long keyHighTempCnt = 0L;
                if (keyedCntState.value() != null) {
                    keyHighTempCnt = keyedCntState.value() + 1;
                    keyedCntState.update(keyHighTempCnt);
                } else {
                    keyedCntState.update(1L);
                }
                out.collect(Tuple3.of(value.id, keyHighTempCnt, opHighTempCnt));
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 初始化键值分区状态
            ValueStateDescriptor<Long> keyCntDescriptor = new ValueStateDescriptor<>("keyedCnt", Types.LONG);
            keyedCntState = context.getKeyedStateStore().getState(keyCntDescriptor);
            // 初始化算子状态
            ListStateDescriptor<Long> opCntDescriptor = new ListStateDescriptor<>("opCnt", Types.LONG);
            opCntState = context.getOperatorStateStore().getListState(opCntDescriptor);
            // 利用算子状态初始化本地的变量
            Iterable<Long> iter = opCntState.get();

            long count = 0;
            for (Long e : iter) {
                count += e;
            }
            opHighTempCnt = count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 利用本地的状态更新算子状态
            opCntState.clear();
            opCntState.add(opHighTempCnt);
        }
    }
}
