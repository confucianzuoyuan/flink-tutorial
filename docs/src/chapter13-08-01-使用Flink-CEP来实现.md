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
