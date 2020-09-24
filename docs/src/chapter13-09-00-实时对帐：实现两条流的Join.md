## 实时对帐：实现两条流的Join

完整代码如下：

**scala version**

```scala
object TwoStreamJoin {

  case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

  case class PayEvent(orderId: String, eventType: String, eventTime: Long)

  val unmatchedOrders = new OutputTag[String]("unmatched-orders")
  val unmatchedPays   = new OutputTag[String]("unmatched-pays")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderStream = env
      .fromElements(
        OrderEvent("order_1", "pay", 1000L),
        OrderEvent("order_2", "pay", 2000L)
      )
      .assignAscendingTimestamps(r => r.eventTime)
      .keyBy(r => r.orderId)

    val payStream = env
      .fromElements(
        PayEvent("order_1", "weixin", 3000L),
        PayEvent("order_3", "weixin", 4000L)
      )
      .assignAscendingTimestamps(r => r.eventTime)
      .keyBy(r => r.orderId)

    val result = orderStream
      .connect(payStream)
      .process(new MatchFunction)

    result.print()

    result.getSideOutput(unmatchedOrders).print()

    result.getSideOutput(unmatchedPays).print()

    env.execute()
  }

  class MatchFunction extends CoProcessFunction[OrderEvent, PayEvent, String] {

    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("order", Types.of[OrderEvent])
    )

    lazy val payState = getRuntimeContext.getState(
      new ValueStateDescriptor[PayEvent]("pay", Types.of[PayEvent])
    )

    override def processElement1(order: OrderEvent, context: CoProcessFunction[OrderEvent, PayEvent, String]#Context, collector: Collector[String]): Unit = {
      val pay = payState.value()

      if (pay != null) {
        payState.clear()
        collector.collect("order id: " + order.orderId + " matched success!")
      } else {
        orderState.update(order)
        context.timerService().registerEventTimeTimer(order.eventTime + 5000L)
      }
    }

    override def processElement2(pay: PayEvent, context: CoProcessFunction[OrderEvent, PayEvent, String]#Context, collector: Collector[String]): Unit = {
      val order = orderState.value()

      if (order != null) {
        orderState.clear()
        collector.collect("order id: " + pay.orderId + " match success!")
      } else {
        payState.update(pay)
        context.timerService().registerEventTimeTimer(pay.eventTime + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, PayEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      if (orderState.value() != null) {
        ctx.output(unmatchedOrders, "order id: " + orderState.value().orderId + " fail match")
        orderState.clear()
      }
      if (payState.value() != null) {
        ctx.output(unmatchedPays, "order id: " + payState.value().orderId + " fail match")
        payState.clear()
      }
    }
  }
}
```

**java version**

```java
public class TwoStreamsJoin {

    private static OutputTag<String> unmatchedOrders = new OutputTag<String>("order"){};
    private static OutputTag<String> unmatchedPays   = new OutputTag<String>("pay"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        KeyedStream<OrderEvent, String> orderStream = env
                .fromElements(
                        new OrderEvent("order_1", "pay", 1000L),
                        new OrderEvent("order_2", "pay", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent orderEvent, long l) {
                                        return orderEvent.eventTime;
                                    }
                                })
                )
                .keyBy(r -> r.orderId);

        KeyedStream<PayEvent, String> payStream = env
                .fromElements(
                        new PayEvent("order_1", "weixin", 3000L),
                        new PayEvent("order_3", "weixin", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PayEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<PayEvent>() {
                                    @Override
                                    public long extractTimestamp(PayEvent payEvent, long l) {
                                        return payEvent.eventTime;
                                    }
                                })
                )
                .keyBy(r -> r.orderId);

        SingleOutputStreamOperator<String> result = orderStream
                .connect(payStream)
                .process(new CoProcessFunction<OrderEvent, PayEvent, String>() {
                    private ValueState<OrderEvent> orderState;
                    private ValueState<PayEvent> payState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        orderState = getRuntimeContext().getState(
                                new ValueStateDescriptor<OrderEvent>("order", OrderEvent.class)
                        );
                        payState = getRuntimeContext().getState(
                                new ValueStateDescriptor<PayEvent>("pay", PayEvent.class)
                        );
                    }

                    @Override
                    public void processElement1(OrderEvent orderEvent, Context context, Collector<String> collector) throws Exception {
                        PayEvent pay = payState.value();
                        if (pay != null) {
                            payState.clear();
                            collector.collect("order id " + orderEvent.orderId + " matched success");
                        } else {
                            orderState.update(orderEvent);
                            context.timerService().registerEventTimeTimer(orderEvent.eventTime + 5000L);
                        }
                    }

                    @Override
                    public void processElement2(PayEvent payEvent, Context context, Collector<String> collector) throws Exception {
                        OrderEvent order = orderState.value();
                        if (order != null) {
                            orderState.clear();
                            collector.collect("order id" + payEvent.orderId + " matched success");
                        } else {
                            payState.update(payEvent);
                            context.timerService().registerEventTimeTimer(payEvent.eventTime + 5000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (orderState.value() != null) {
                            ctx.output(unmatchedOrders, "order id: " + orderState.value().orderId + " not match");
                            orderState.clear();
                        }
                        if (payState.value() != null) {
                            ctx.output(unmatchedPays, "order id: " + payState.value().orderId + " not match");
                        }
                    }
                });

        result.print();

        result.getSideOutput(unmatchedOrders).print();

        result.getSideOutput(unmatchedPays).print();

        env.execute();
    }
}
```
