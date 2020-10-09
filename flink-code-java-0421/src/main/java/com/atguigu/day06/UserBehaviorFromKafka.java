package com.atguigu.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

// 先启动消费者程序
public class UserBehaviorFromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        SingleOutputStreamOperator<UserBehavior> stream = env
                .addSource(new FlinkKafkaConsumer011<String>(
                        "hotitems",
                        new SimpleStringSchema(),
                        properties
                ))
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String s) throws Exception {
                        String[] arr = s.split(",");
                        return new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                                        return userBehavior.timestamp;
                                    }
                                })
                );

        stream
                .keyBy(r -> r.itemId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(r -> r.windowEnd)
                .process(new TopN(3))
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            collector.collect(new ItemViewCount(key, iterable.iterator().next(), ctx.window().getStart(), ctx.window().getEnd()));
        }
    }

    public static class TopN extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private ListState<ItemViewCount> itemState;
        private Integer threshold;

        public TopN(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("item-state", ItemViewCount.class)
            );
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            itemState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.windowEnd + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Iterable<ItemViewCount> itemViewCounts = itemState.get();
            ArrayList<ItemViewCount> itemList = new ArrayList<>();
            for (ItemViewCount iter : itemViewCounts) {
                itemList.add(iter);
            }
            itemState.clear();

            itemList.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount t0, ItemViewCount t1) {
                    return t1.count.intValue() - t0.count.intValue();
                }
            });

            StringBuilder result = new StringBuilder();
            result
                    .append("===========================================\n")
                    .append("time: ")
                    .append(new Timestamp(timestamp - 100L))
                    .append("\n");
            for (int i = 0; i < this.threshold; i++) {
                ItemViewCount currItem = itemList.get(i);
                result
                        .append("No.")
                        .append(i + 1)
                        .append(" : ")
                        .append(currItem.itemId)
                        .append(" count = ")
                        .append(currItem.count)
                        .append("\n");
            }
            result
                    .append("===========================================\n\n\n");
            Thread.sleep(1000L);
            out.collect(result.toString());
        }
    }
}
