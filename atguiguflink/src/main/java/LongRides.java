import atguigu.datatypes.TaxiRide;
import atguigu.sources.CheckpointedTaxiRideSource;
import atguigu.utils.ProjectBase;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 *
 * 出租车报警系统：报警条件为 START 事件在2小时之内都没有匹配的 END 事件发生
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class LongRides extends ProjectBase {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ProjectBase.pathToRideData);

        // 将时间轴压缩, 10分钟 => 1秒钟
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置流时间为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ProjectBase.parallelism);

        // CheckpointedTaxiRideSource delivers events in order
        // 形成流
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new CheckpointedTaxiRideSource(input, servingSpeedFactor)));

        // 使用rideId分组, 形成keyedStream, rideId为某次出行的id, 正常应该有START事件和END事件
        DataStream<TaxiRide> keyedRides = rides
                .keyBy("rideId");

        // A complete taxi ride has a START event followed by an END event
        Pattern<TaxiRide, TaxiRide> completedRides =
                Pattern.<TaxiRide>begin("start")
                        .where(new SimpleCondition<TaxiRide>() {
                            @Override
                            public boolean filter(TaxiRide ride) throws Exception {
                                return ride.isStart;
                            }
                        })
                        // 两个事件必须挨在一起
                        .next("end")
                        .where(new SimpleCondition<TaxiRide>() {
                            @Override
                            public boolean filter(TaxiRide ride) throws Exception {
                                return !ride.isStart;
                            }
                        });

        // We want to find rides that have NOT been completed within 120 minutes.
        // This pattern matches rides that ARE completed.
        // Below we will ignore rides that match this pattern, and emit those that timeout.
        // 先找出正常的出行ride
        PatternStream<TaxiRide> patternStream = CEP.pattern(keyedRides, completedRides.within(Time.minutes(120)));

        OutputTag<TaxiRide> timedout = new OutputTag<TaxiRide>("timedout"){};

        // flatSelect可以提取多个无法模式匹配的事件
        SingleOutputStreamOperator<TaxiRide> longRides = patternStream.flatSelect(
                timedout,
                new TaxiRideTimedOut<TaxiRide>(),
                new FlatSelectNothing<TaxiRide>()
        );

        printOrTest(longRides.getSideOutput(timedout));

        env.execute("Long Taxi Rides (CEP)");
    }

    /** 将超时的ride提取出来, 需要override一下timeout函数 */
    public static class TaxiRideTimedOut<TaxiRide> implements PatternFlatTimeoutFunction<TaxiRide, TaxiRide> {
        @Override
        public void timeout(Map<String, List<TaxiRide>> map, long l, Collector<TaxiRide> collector) throws Exception {
            // 只需要打印start这个事件就可以了
            TaxiRide rideStarted = map.get("start").get(0);
            collector.collect(rideStarted);
        }
    }

    public static class FlatSelectNothing<T> implements PatternFlatSelectFunction<T, T> {
        @Override
        public void flatSelect(Map<String, List<T>> pattern, Collector<T> collector) {
        }
    }
}
