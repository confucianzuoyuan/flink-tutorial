## APP分渠道数据统计

完整代码如下：

```java
package com.atguigu

import java.util.{Calendar, UUID}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object AppMarketingByChannel {
  case class MarketingUserBehavior(userId: String,
                                   behavior: String,
                                   channel: String,
                                   ts: Long)

  class SimulatedEventSource
    extends RichParallelSourceFunction[MarketingUserBehavior] {

    var running = true

    val channelSet = Seq("AppStore", "XiaomiStore")
    val behaviorTypes = Seq("BROWSE", "CLICK")
    val rand = new Random

    override def run(ctx: SourceContext[MarketingUserBehavior]): Unit = {
      while (running) {
        val userId = UUID.randomUUID().toString
        val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
        val channel = channelSet(rand.nextInt(channelSet.size))
        val ts = Calendar.getInstance().getTimeInMillis

        ctx.collect(MarketingUserBehavior(userId, behaviorType, channel, ts))

        Thread.sleep(10)
      }
    }

    override def cancel(): Unit = running = false
  }

  def main(args: Array[String]): Unit = {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior != "UNINSTALL")
      .map(r => {
        ((r.channel, r.behavior), 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .process(new MarketingCountByChannel)
    stream.print()
    env.execute()
  }

  class MarketingCountByChannel
    extends ProcessWindowFunction[((String, String), Long),
      (String, Long, Long), (String, String), TimeWindow] {
    override def process(key:  (String,String),
                         context: Context,
                         elements: Iterable[((String, String), Long)],
                         out: Collector[(String, Long, Long)]): Unit = {
      out.collect((key._1, elements.size, context.window.getEnd))
    }
  }
}
```

