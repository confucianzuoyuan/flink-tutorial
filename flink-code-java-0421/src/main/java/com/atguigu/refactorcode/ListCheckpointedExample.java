package com.atguigu.refactorcode;

import com.atguigu.day02.util.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

public class ListCheckpointedExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.execute();
    }

    public static class HighTempCounter extends RichFlatMapFunction<SensorReading, Tuple2<Integer, Long>> implements ListCheckpointed<Long> {
        private int subTaskIdx;
        private long highTempCnt = 0L;
        private double threshold;

        public HighTempCounter(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            subTaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple2<Integer, Long>> out) throws Exception {
           if (value.temperature > this.threshold) {
               highTempCnt += 1;
               out.collect(Tuple2.of(subTaskIdx, highTempCnt));
           }
        }

        @Override
        public void restoreState(List<Long> state) throws Exception {
            highTempCnt = 0;
            for (Long l : state) {
                highTempCnt += l;
            }
        }

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(highTempCnt);
        }
    }
}
