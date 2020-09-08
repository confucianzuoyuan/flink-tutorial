### 将事件发送到侧输出

大部分的DataStream API的算子的输出是单一输出，也就是某种数据类型的流。除了split算子，可以将一条流分成多条流，这些流的数据类型也都相同。process function的side outputs功能可以产生多条流，并且这些流的数据类型可以不一样。一个side output可以定义为OutputTag[X]对象，X是输出流的数据类型。process function可以通过Context对象发射一个事件到一个或者多个side outputs。

例子

```java
public class StreamingJob {

	private static OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		DataStream<SensorReading> readings = env.addSource(new SensorSource());

		SingleOutputStreamOperator<SensorReading> monitoredReadings = readings
				.process(new FreezingMonitor());

		monitoredReadings.getSideOutput(outputTag).print();

		env.execute("Flink Streaming Java API Skeleton");
	}

	public static class FreezingMonitor extends ProcessFunction<SensorReading, SensorReading> {
		// 定义一个侧输出标签

		@Override
		public void processElement(SensorReading r, Context ctx, Collector<SensorReading> out) throws Exception {
			if (r.temperature < 32.0) {
				ctx.output(outputTag, "Freezing Alarm for " + r.id);
			}
			out.collect(r);
		}
	}
}
```

