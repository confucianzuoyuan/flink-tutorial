### CoProcessFunction

对于两条输入流，DataStream API提供了CoProcessFunction这样的low-level操作。CoProcessFunction提供了操作每一个输入流的方法: processElement1()和processElement2()。类似于ProcessFunction，这两种方法都通过Context对象来调用。这个Context对象可以访问事件数据，定时器时间戳，TimerService，以及side outputs。CoProcessFunction也提供了onTimer()回调函数。下面的例子展示了如何使用CoProcessFunction来合并两条流。

**scala version**

```scala
object SensorSwitch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource).keyBy(r => r.id)

    val switches = env.fromElements(("sensor_2", 10 * 1000L)).keyBy(r => r._1)

    stream
      .connect(switches)
      .process(new SwitchProcess)
      .print()

    env.execute()
  }

  class SwitchProcess extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {

    lazy val forwardSwitch = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("switch", Types.of[Boolean])
    )

    override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (forwardSwitch.value()) {
        out.collect(value)
      }
    }

    override def processElement2(value: (String, Long), ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      forwardSwitch.update(true)
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + value._2)
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      forwardSwitch.clear()
    }
  }
}
```

**java version**

```java
public class SensorSwitch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<SensorReading, String> stream = env
                .addSource(new SensorSource())
                .keyBy(r -> r.id);

        KeyedStream<Tuple2<String, Long>, String> switches = env
                .fromElements(Tuple2.of("sensor_2", 10 * 1000L))
                .keyBy(r -> r.f0);

        stream
                .connect(switches)
                .process(new SwitchProcess())
                .print();

        env.execute();
    }

    public static class SwitchProcess extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {

        private ValueState<Boolean> forwardingEnabled;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            forwardingEnabled = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("filterSwitch", Types.BOOLEAN)
            );
        }

        @Override
        public void processElement1(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if (forwardingEnabled.value() != null && forwardingEnabled.value()) {
                out.collect(value);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<SensorReading> out) throws Exception {
            forwardingEnabled.update(true);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + value.f1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            forwardingEnabled.clear();
        }
    }
}
```

