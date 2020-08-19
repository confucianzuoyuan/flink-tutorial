package chapter5;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RollingSum {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<Integer, Integer, Integer>> inputStream = env.fromElements(
                Tuple3.of(1, 2, 2), Tuple3.of(2, 3, 1), Tuple3.of(2, 2, 4), Tuple3.of(1, 5, 3));

        DataStream<Tuple3<Integer, Integer, Integer>> resultStream = inputStream
                .keyBy(0) // key on first field of tuples
                .sum(1); // sum the second field of the tuple

        resultStream.print();

        // execute the application
        env.execute();
    }

}
