### 时间服务和定时器

Context和OnTimerContext所持有的TimerService对象拥有以下方法:

* `currentProcessingTime(): Long` 返回当前处理时间
* `currentWatermark(): Long` 返回当前水位线的时间戳
* `registerProcessingTimeTimer(timestamp: Long): Unit` 会注册当前key的processing time的timer。当processing time到达定时时间时，触发timer。
* `registerEventTimeTimer(timestamp: Long): Unit` 会注册当前key的event time timer。当水位线大于等于定时器注册的时间时，触发定时器执行回调函数。
* `deleteProcessingTimeTimer(timestamp: Long): Unit` 删除之前注册处理时间定时器。如果没有这个时间戳的定时器，则不执行。
* `deleteEventTimeTimer(timestamp: Long): Unit` 删除之前注册的事件时间定时器，如果没有此时间戳的定时器，则不执行。

当定时器timer触发时，执行回调函数onTimer()。processElement()方法和onTimer()方法是同步（不是异步）方法，这样可以避免并发访问和操作状态。

针对每一个key和timestamp，只能注册一个定期器。也就是说，每一个key可以注册多个定时器，但在每一个时间戳只能注册一个定时器。KeyedProcessFunction默认将所有定时器的时间戳放在一个优先队列中。在Flink做检查点操作时，定时器也会被保存到状态后端中。

举个例子说明KeyedProcessFunction如何操作KeyedStream。

下面的程序展示了如何监控温度传感器的温度值，如果温度值在一秒钟之内(processing time)连续上升，报警。

**scala version**

```scala
val warnings = readings
  .keyBy(r => r.id)
  .process(new TempIncreaseAlertFunction)
```

```scala
  class TempIncrease extends KeyedProcessFunction[String, SensorReading, String] {
    // 懒加载；
    // 状态变量会在检查点操作时进行持久化，例如hdfs
    // 只会初始化一次，单例模式
    // 在当机重启程序时，首先去持久化设备寻找名为`last-temp`的状态变量，如果存在，则直接读取。不存在，则初始化。
    // 用来保存最近一次温度
    // 默认值是0.0
    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
    )

    // 默认值是0L
    lazy val timer: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      // 使用`.value()`方法取出最近一次温度值，如果来的温度是第一条温度，则prevTemp为0.0
      val prevTemp = lastTemp.value()
      // 将到来的这条温度值存入状态变量中
      lastTemp.update(value.temperature)

      // 如果timer中有定时器的时间戳，则读取
      val ts = timer.value()

      if (prevTemp == 0.0 || value.temperature < prevTemp) {
        ctx.timerService().deleteProcessingTimeTimer(ts)
        timer.clear()
      } else if (value.temperature > prevTemp && ts == 0) {
        val oneSecondLater = ctx.timerService().currentProcessingTime() + 1000L
        ctx.timerService().registerProcessingTimeTimer(oneSecondLater)
        timer.update(oneSecondLater)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("传感器ID是 " + ctx.getCurrentKey + " 的传感器的温度连续1s上升了！")
      timer.clear()
    }
  }
```

**java version**

```java
DataStream<String> warings = readings
    .keyBy(r -> r.id)
    .process(new TempIncreaseAlertFunction());
```

看一下TempIncreaseAlertFunction如何实现, 程序中使用了ValueState这样一个状态变量, 后面会详细讲解。

```java
	public static class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {

		private ValueState<Double> lastTemp;
		private ValueState<Long> currentTimer;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			lastTemp = getRuntimeContext().getState(
					new ValueStateDescriptor<>("last-temp", Types.DOUBLE)
			);
			currentTimer = getRuntimeContext().getState(
					new ValueStateDescriptor<>("current-timer", Types.LONG)
			);
		}

		@Override
		public void processElement(SensorReading r, Context ctx, Collector<String> out) throws Exception {
			// 取出上一次的温度
			Double prevTemp = 0.0;
			if (lastTemp.value() != null) {
				prevTemp = lastTemp.value();
			}
			// 将当前温度更新到上一次的温度这个变量中
			lastTemp.update(r.temperature);

			Long curTimerTimestamp = 0L;
			if (currentTimer.value() != null) {
				curTimerTimestamp = currentTimer.value();
			}
			if (prevTemp == 0.0 || r.temperature < prevTemp) {
				// 温度下降或者是第一个温度值，删除定时器
				ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
				// 清空状态变量
				currentTimer.clear();
			} else if (r.temperature > prevTemp && curTimerTimestamp == 0) {
				// 温度上升且我们并没有设置定时器
				long timerTs = ctx.timerService().currentProcessingTime() + 1000L;
				ctx.timerService().registerProcessingTimeTimer(timerTs);
				// 保存定时器时间戳
				currentTimer.update(timerTs);
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
			super.onTimer(timestamp, ctx, out);
			out.collect("传感器id为: "
					+ ctx.getCurrentKey()
					+ "的传感器温度值已经连续1s上升了。");
			currentTimer.clear();
		}
	}
```

