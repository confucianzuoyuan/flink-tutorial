/** 需求：找到起点和终点都在纽约的出租车行程 */


import atguigu.datatypes.TaxiRide;
import atguigu.sources.TaxiRideSource;
import atguigu.utils.GeoUtils;
import atguigu.utils.ProjectBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideCleansing extends ProjectBase {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", pathToRideData);

        /** 最多允许乱序为60秒 */
        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        /** 将时间轴压缩，10分钟 -> 1秒钟 */
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ProjectBase.parallelism);

        /** 添加数据源 */
        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

        DataStream<TaxiRide> filteredRides = rides
                // keep only those rides and both start and end in NYC
                .filter(new NYCFilter());

        // print the filtered stream
        printOrTest(filteredRides);

        // run the cleansing pipeline
        env.execute("Taxi Ride Cleansing");
    }

    public static class NYCFilter implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {

            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                    GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }
}
