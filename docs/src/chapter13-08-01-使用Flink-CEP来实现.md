### 使用Flink CEP来实现

在电商平台中，最终创造收入和利润的是用户下单购买的环节；更具体一点，是用户真正完成支付动作的时候。用户下单的行为可以表明用户对商品的需求，但在现实中，并不是每次下单都会被用户立刻支付。当拖延一段时间后，用户支付的意愿会降低。所以为了让用户更有紧迫感从而提高支付转化率，同时也为了防范订单支付环节的安全风险，电商网站往往会对订单状态进行监控，设置一个失效时间（比如15分钟），如果下单后一段时间仍未支付，订单就会被取消。

我们将会利用CEP库来实现这个功能。我们先将事件流按照订单号orderId分流，然后定义这样的一个事件模式：在15分钟内，事件“create”与“pay”严格紧邻：

```scala
val orderPayPattern = Pattern.begin[OrderEvent]("begin")
  .where(_.eventType == "create")
  .next("next")
  .where(_.eventType == "pay")
  .within(Time.seconds(5))
```

这样调用`.select`方法时，就可以同时获取到匹配出的事件和超时未匹配的事件了。
在src/main/scala下继续创建OrderTimeout.scala文件，新建一个单例对象。定义样例类OrderEvent，这是输入的订单事件流；另外还有OrderResult，这是输出显示的订单状态结果。由于没有现成的数据，我们还是用几条自定义的示例数据来做演示。

完整代码如下：

**scala version**

```scala
object OrderTimeoutDetect {
   case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val timeoutputTag = new OutputTag[String]("timeout-tag")

    val orderStream = env
      .fromElements(
        OrderEvent("order_1", "create", 2000L),
        OrderEvent("order_2", "create", 3000L),
        OrderEvent("order_1", "pay", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(r => r.orderId)

    val pattern = Pattern
      .begin[OrderEvent]("create")
      .where(_.eventType.equals("create"))
      .next("pay")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val patternedStream = CEP.pattern(orderStream, pattern)

    val selectFunc = (map: scala.collection.Map[String, Iterable[OrderEvent]], out: Collector[String]) => {
      val create = map("create").iterator.next()
      out.collect("order id " + create.orderId + " is payed!")
    }

    val timeoutFunc = (map: scala.collection.Map[String, Iterable[OrderEvent]], ts: Long, out: Collector[String]) => {
      val create = map("create").iterator.next()
      out.collect("order id " + create.orderId + " is not payed! and timeout ts is " + ts)
    }

    val selectStream = patternedStream.flatSelect(timeoutputTag)(timeoutFunc)(selectFunc)

    selectStream.print()
    selectStream.getSideOutput(timeoutputTag).print()

    env.execute()
  }
}
```

**java version**

POJO类实现

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
public class OrderTimeoutDetect {
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

        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.seconds(5));

        PatternStream<OrderEvent> patternStream = CEP.pattern(stream.keyBy(r -> r.orderId), pattern);

        SingleOutputStreamOperator<String> result = patternStream
                .select(
                        new OutputTag<String>("order-timeout") {
                        },
                        new PatternTimeoutFunction<OrderEvent, String>() {
                            @Override
                            public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                                return "订单ID为 " + map.get("create").get(0).orderId + " 没有支付！";
                            }
                        },
                        new PatternSelectFunction<OrderEvent, String>() {
                            @Override
                            public String select(Map<String, List<OrderEvent>> map) throws Exception {
                                return "订单ID为 " + map.get("pay").get(0).orderId + " 已经支付！";
                            }
                        }
                );

        result.print();

        result.getSideOutput(new OutputTag<String>("order-timeout") {}).print();

        env.execute();
    }
}
```
