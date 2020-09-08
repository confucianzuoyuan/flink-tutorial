### 使用Flink CEP来实现

在电商平台中，最终创造收入和利润的是用户下单购买的环节；更具体一点，是用户真正完成支付动作的时候。用户下单的行为可以表明用户对商品的需求，但在现实中，并不是每次下单都会被用户立刻支付。当拖延一段时间后，用户支付的意愿会降低。所以为了让用户更有紧迫感从而提高支付转化率，同时也为了防范订单支付环节的安全风险，电商网站往往会对订单状态进行监控，设置一个失效时间（比如15分钟），如果下单后一段时间仍未支付，订单就会被取消。

我们将会利用CEP库来实现这个功能。我们先将事件流按照订单号orderId分流，然后定义这样的一个事件模式：在15分钟内，事件“create”与“pay”严格紧邻：

```java
val orderPayPattern = Pattern.begin[OrderEvent]("begin")
  .where(_.eventType == "create")
  .next("next")
  .where(_.eventType == "pay")
  .within(Time.seconds(5))
```

这样调用.select方法时，就可以同时获取到匹配出的事件和超时未匹配的事件了。
在src/main/scala下继续创建OrderTimeout.scala文件，新建一个单例对象。定义样例类OrderEvent，这是输入的订单事件流；另外还有OrderResult，这是输出显示的订单状态结果。由于没有现成的数据，我们还是用几条自定义的示例数据来做演示。
完整代码如下：

```java
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.util.Collector
import scala.collection.Map

case class OrderEvent(orderId: String, eventType: String, eventTime: String)

object OrderTimeout {

  def main(args: Array[String]): Unit = {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream = env.fromCollection(List(
      OrderEvent("1", "create", "1558430842"),
      OrderEvent("2", "create", "1558430843"),
      OrderEvent("2", "pay", "1558430844"),
      OrderEvent("3", "pay", "1558430942"),
      OrderEvent("4", "pay", "1558430943")
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)

//    val orders: DataStream[String] = env
//      .socketTextStream("localhost", 9999)
//
//    val orderEventStream = orders
//      .map(s => {
//        println(s)
//        val slist = s.split("\\|")
//        println(slist)
//        OrderEvent(slist(0), slist(1), slist(2))
//      })
//      .assignAscendingTimestamps(_.eventTime.toLong * 1000)

    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType.equals("create"))
      .next("next")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val orderTimeoutOutput = OutputTag[OrderEvent]("orderTimeout")

    val patternStream = CEP.pattern(
      orderEventStream.keyBy("orderId"), orderPayPattern)

    val timeoutFunction = (
      map: Map[String, Iterable[OrderEvent]],
      timestamp: Long,
      out: Collector[OrderEvent]
    ) => {
      print(timestamp)
      val orderStart = map.get("begin").get.head
      out.collect(orderStart)
    }

    val selectFunction = (
      map: Map[String, Iterable[OrderEvent]],
      out: Collector[OrderEvent]
    ) => {}

    val timeoutOrder = patternStream
      .flatSelect(orderTimeoutOutput)(timeoutFunction)(selectFunction)

    timeoutOrder.getSideOutput(orderTimeoutOutput).print()

    env.execute

  }
}
```

