### CoProcessFunction

对于两条输入流，DataStream API提供了CoProcessFunction这样的low-level操作。CoProcessFunction提供了操作每一个输入流的方法: processElement1()和processElement2()。类似于ProcessFunction，这两种方法都通过Context对象来调用。这个Context对象可以访问事件数据，定时器时间戳，TimerService，以及side outputs。CoProcessFunction也提供了onTimer()回调函数。下面的例子展示了如何使用CoProcessFunction来合并两条流。

```java
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		DataStream<SensorReading> readings = env.addSource(new SensorSource());

		DataStream<Tuple2<String, Long>> filterSwitches = env.fromElements(
						new Tuple2("sensor_2", 10 * 1000L),
						new Tuple2("sensor_7", 60 * 1000L));

		DataStream<SensorReading> forwardedReadings = readings
				// connect readings and switches
				.connect(filterSwitches)
				// key by sensor ids
				.keyBy(r -> r.id, r -> r.f0)
				// apply filtering CoProcessFunction
				.process(new ReadingFilter());

		forwardedReadings.print();

		env.execute("Flink Streaming Java API Skeleton");
	}

	public static class ReadingFilter
			extends CoProcessFunction<SensorReading,
						Tuple2<String, Long>, SensorReading> {
		// switch to enable forwarding
		// 传送数据的开关
		private ValueState<Boolean> forwardingEnabled;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			forwardingEnabled = getRuntimeContext().getState(
					new ValueStateDescriptor<>("filterSwitch", Types.BOOLEAN)
			);
		}

		@Override
		public void processElement1(SensorReading r, Context ctx, Collector<SensorReading> out) throws Exception {
			// 决定我们是否要将数据继续传下去
			if (forwardingEnabled.value() != null && forwardingEnabled.value()) {
				out.collect(r);
			}
		}

		@Override
		public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<SensorReading> out) throws Exception {
			// 允许继续传输数据
			forwardingEnabled.update(true);
			long timerTimestamp = ctx.timerService().currentProcessingTime() + value.f1;
			ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
			super.onTimer(timestamp, ctx, out);
			forwardingEnabled.clear();
		}
	}
}
```

