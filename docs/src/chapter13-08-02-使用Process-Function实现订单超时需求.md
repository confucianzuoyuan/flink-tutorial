### 使用Process Function实现订单超时需求

**scala version**

```scala
object OrderTimeoutWithoutCep {

  case class OrderEvent(orderId: String,
                        eventType: String,
                        eventTime: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .fromElements(
        OrderEvent("1", "create", "2"),
        OrderEvent("2", "create", "3"),
        OrderEvent("2", "pay", "4")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)
      .process(new OrderMatchFunc)

    stream.print()
    env.execute()
  }

  class OrderMatchFunc extends KeyedProcessFunction[String, OrderEvent, String] {
    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("saved order", Types.of[OrderEvent])
    )

    override def processElement(value: OrderEvent,
                                ctx: KeyedProcessFunction[String, OrderEvent, String]#Context,
                                out: Collector[String]): Unit = {
      if (value.eventType.equals("create")) {
        if (orderState.value() == null) { // 为什么要判空？因为可能出现`pay`先到的情况
          // 如果orderState为空，保存`create`事件
          orderState.update(value)
        }
      } else {
        // 保存`pay`事件
        orderState.update(value)
      }

      ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5000L)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, OrderEvent, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      val savedOrder = orderState.value()

      if (savedOrder != null && savedOrder.eventType.equals("create")) {
        out.collect("超时订单的ID为：" + savedOrder.orderId)
      }

      orderState.clear()
    }
  }
}
```

**java version**

OrderEvent的POJO类实现

```java
public class OrderEvent {
    public String orderId;
    public String eventType;
    public Long eventTime;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String eventType, Long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
```

```java
public class OrderTimeoutDetectWithoutCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> stream = env
                .fromElements(
                        new OrderEvent("order_1", "create", 1000L),
                        new OrderEvent("order_2", "create", 2000L),
                        new OrderEvent("order_1", "pay", 3000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                );

        stream
                .keyBy(r -> r.orderId)
                .process(new MyKeyed())
                .print();

        env.execute();
    }

    public static class MyKeyed extends KeyedProcessFunction<String, OrderEvent, String> {

        private ValueState<OrderEvent> orderState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order", OrderEvent.class));
        }

        @Override
        public void processElement(OrderEvent orderEvent, Context context, Collector<String> collector) throws Exception {
            if (orderEvent.eventType.equals("create")) {
                // 为什么判空？因为pay事件可能先到
                if (orderState.value() == null) {
                    orderState.update(orderEvent);
                    context.timerService().registerEventTimeTimer(orderEvent.eventTime + 5000L);
                }
            } else {
                orderState.update(orderEvent);
                collector.collect("订单ID为 " + orderEvent.orderId + " 支付成功！");
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (orderState.value() != null && orderState.value().eventType.equals("create")) {
                out.collect("没有支付的订单ID是 " + orderState.value().orderId);
            }
            orderState.clear();
        }
    }
}
```
