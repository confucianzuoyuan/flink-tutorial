package com.atguigu.day10;

import com.atguigu.day06.ItemViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class ApacheLogAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<ApacheLogEvent> stream = env
                .readTextFile("/home/zuoyuan/flink-tutorial/flink-code-java-0421/src/main/resources/apachelog.txt")
                .map(new MapFunction<String, ApacheLogEvent>() {
                    @Override
                    public ApacheLogEvent map(String line) throws Exception {
                        String[] linearray = line.split(" ");
                        // 把时间戳ETL成毫秒
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                        long timestamp = simpleDateFormat.parse(linearray[3]).getTime();
                        return new ApacheLogEvent(linearray[0], linearray[2], timestamp, linearray[5], linearray[6]);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLogEvent>() {
                            @Override
                            public long extractTimestamp(ApacheLogEvent element, long recordTimestamp) {
                                return element.eventTime;
                            }
                        }));

        stream
                .keyBy(r -> r.url)
                .timeWindow(Time.seconds(60), Time.seconds(5))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(r -> r.windowEnd)
                .process(new TopNUrl(1))
                .print();

        env.execute();
    }

    public static class TopNUrl extends KeyedProcessFunction<Long, UrlViewCount, String> {
        private ListState<UrlViewCount> urlState;
        private Integer rank;
        public TopNUrl(Integer rank) {
            this.rank = rank;
        }
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            urlState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("url-state", UrlViewCount.class));
        }

        @Override
        public void processElement(UrlViewCount urlViewCount, Context context, Collector<String> collector) throws Exception {
            urlState.add(urlViewCount);
            context.timerService().registerEventTimeTimer(urlViewCount.windowEnd + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Iterable<UrlViewCount> urlViewCounts = urlState.get();
            ArrayList<UrlViewCount> urlList = new ArrayList<>();
            for (UrlViewCount iter : urlViewCounts) {
                urlList.add(iter);
            }
            urlState.clear();

            urlList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount t0, UrlViewCount t1) {
                    return t1.count.intValue() - t0.count.intValue();
                }
            });

            StringBuilder result = new StringBuilder();
            result
                    .append("===========================================\n")
                    .append("time: ")
                    .append(new Timestamp(timestamp - 100L))
                    .append("\n");
            for (int i = 0; i < this.rank; i++) {
                UrlViewCount currItem = urlList.get(i);
                result
                        .append("No.")
                        .append(i + 1)
                        .append(" : ")
                        .append(currItem.url)
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

    public static class WindowResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
            collector.collect(new UrlViewCount(s, context.window().getEnd(), iterable.iterator().next()));
        }
    }

    public static class CountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
}
