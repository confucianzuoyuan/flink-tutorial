import org.apache.flink.api.common.functions.FlatMapFunction;
// Tuple1 ~ Tuple25
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/** 统计滚动窗口的词频
 *  DataSet DataStream
 * */
public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过连接 socket 获取输入数据，这里连接到本地9000端口，如果9000端口已被占用，请换一个端口
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

        // 解析数据，按 word 分组，开窗，聚合
        // Tuple2为flink框架中的自定义类型, 元组的元素数量是2个
        DataStream<Tuple2<String, Integer>> windowCounts = text
                // flatMap == flatten + map
                // [[1,2,3],[4,5,6]] => [1,2,3,4,5,6]
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        // 使用空白符进行切割，并且遍历每一个单词
                        for (String word : value.split("\\s")) {
                            // 实例化一个Tuple2并收集, 这一步实现了mr中的map
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                // 使用Tuple2中的第0个元素为key进行聚合
                .keyBy(0)
                // tumbling window: 滚动窗口
                // [9:00:00, 9:00:05), [9:00:05, 9:00:10)
                .timeWindow(Time.seconds(5))
                // 为每个key每个窗口指定了sum聚合函数，在我们的例子中是按照次数字段（即1号索引字段）相加。
                // 实现了mr的reduce语义
                .sum(1);

        // 将结果打印到控制台，注意这里使用的是单线程打印，而非多线程
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }
}
