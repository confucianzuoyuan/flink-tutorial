import atguigu.datatypes.TaxiRide;
import atguigu.sources.TaxiRideSource;
import atguigu.utils.ProjectBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Random;

/**
 *
 * 广播打车的人的位置出去，观察taxi rides流，然后找出距离打车的人最近的并且已完成行程的出租车。
 *
 * Parameters:
 * -input path-to-input-file
 *
 * Use nc -lk 9999 to establish a socket stream from stdin on port 9999
 *
 * Some good locations:
 *
 * -74, 41 					(Near, but outside the city to the NNW)
 * -73.7781, 40.6413 		(JFK Airport)
 * -73.977664, 40.761484	(Museum of Modern Art)
 */
public class NearestTaxi extends ProjectBase {

    private static class Query {

        private final long queryId;
        private final float longitude;
        private final float latitude;

        Query(final float longitude, final float latitude) {
            this.queryId = new Random().nextLong();
            this.longitude = longitude;
            this.latitude = latitude;
        }

        Long getQueryId() {
            return queryId;
        }

        public float getLongitude() {
            return longitude;
        }

        public float getLatitude() {
            return latitude;
        }

        @Override
        public String toString() {
            return "Query{" +
                    "id=" + queryId +
                    ", longitude=" + longitude +
                    ", latitude=" + latitude +
                    '}';
        }
    }

    final static MapStateDescriptor queryDescriptor = new MapStateDescriptor<>(
            "queries",
            BasicTypeInfo.LONG_TYPE_INFO,
            TypeInformation.of(Query.class));

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ProjectBase.pathToRideData);

        final int maxEventDelay = 60;       	// events are out of order by at most 60 seconds
        final int servingSpeedFactor = 600; 	// 10 minutes worth of events are served every second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ProjectBase.parallelism);

        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

        // add a socket source
        BroadcastStream<Query> queryStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Query>() {
                    @Override
                    public Query map(String msg) throws Exception {
                        String[] parts = msg.split(",\\s*");
                        return new Query(
                                Float.valueOf(parts[0]),	// longitude
                                Float.valueOf(parts[1]));	// latitude
                    }
                })
                .broadcast(queryDescriptor);

        DataStream<Tuple3<Long, Long, Float>> reports = rides
                // 根据taxiId分组
                .keyBy((TaxiRide ride) -> ride.taxiId)
                // 连接查询流
                .connect(queryStream)
                // 执行查询
                .process(new QueryFunction());

        DataStream<Tuple3<Long, Long, Float>> nearest = reports
                // key by the queryId
                // 根据 queryId 分组
                .keyBy(new KeySelector<Tuple3<Long, Long, Float>, Long>() {
                    @Override
                    public Long getKey(Tuple3<Long, Long, Float> value) throws Exception {
                        return value.f0;
                    }
                })
                // 找出最近的出租车
                .process(new ClosestTaxi());

        printOrTest(nearest);

        env.execute("Nearest Available Taxi");
    }

    // Only pass thru values that are new minima -- remove duplicates.
    // KeyedProcessFunction<K, I, O>; type of Key; type of Input; type of Output;
    public static class ClosestTaxi extends KeyedProcessFunction<Long, Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>> {
        // store (taxiId, distance), keyed by queryId
        private transient ValueState<Tuple2<Long, Float>> closest;

        // lifecycle开始的初始化操作
        @Override
        public void open(Configuration parameters) throws Exception {
            // 值的状态的描述符
            ValueStateDescriptor<Tuple2<Long, Float>> descriptor =
                    new ValueStateDescriptor<Tuple2<Long, Float>>(
                            // state name; 状态的名字
                            "report",
                            // type information of state; 状态的类型信息
                            TypeInformation.of(new TypeHint<Tuple2<Long, Float>>() {}));
            // 拿到运行时的上下文，再拿到值的状态，并赋值给closest变量
            closest = getRuntimeContext().getState(descriptor);
        }

        @Override
        // in and out tuples: (queryId, taxiId, distance)
        public void processElement(Tuple3<Long, Long, Float> report, Context ctx, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            // 如果closest值为null, 或者report中的距离f2小于closest状态变量中的距离值f1, 则更新距离值和taxiId
            if (closest.value() == null || report.f2 < closest.value().f1) {
                closest.update(new Tuple2<>(report.f1, report.f2));
                out.collect(report);
            }
        }
    }

    // Note that in order to have consistent results after a restore from a checkpoint, the
    // behavior of this method must be deterministic, and NOT depend on characterisitcs of an
    // individual sub-task.
    public static class QueryFunction extends KeyedBroadcastProcessFunction<Long, TaxiRide, Query, Tuple3<Long, Long, Float>> {

        // 处理的是广播元素
        @Override
        public void processBroadcastElement(Query query, Context ctx, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            System.out.println("new query " + query);
            ctx.getBroadcastState(queryDescriptor).put(query.getQueryId(), query);
        }

        @Override
        // Output (queryId, taxiId, euclidean distance) for every query, if the taxi ride is now ending.
        public void processElement(TaxiRide ride, ReadOnlyContext ctx, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            // 已经结束的ride
            if (!ride.isStart) {
                // 广播状态中的值，定义为entry的迭代器
                Iterable<Map.Entry<Long, Query>> entries = ctx.getBroadcastState(queryDescriptor).immutableEntries();

                for (Map.Entry<Long, Query> entry: entries) {
                    final Query query = entry.getValue();
                    // 本次结束的ride所在位置与打车的人的位置的距离
                    final float kilometersAway = (float) ride.getEuclideanDistance(query.getLongitude(), query.getLatitude());

                    out.collect(new Tuple3<>(
                            query.getQueryId(),
                            ride.taxiId,
                            kilometersAway
                    ));
                }
            }
        }
    }
}