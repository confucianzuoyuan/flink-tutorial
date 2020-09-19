## Uv统计的布隆过滤器实现

完整代码如下：

```java
package com.atguigu.proj

import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction

import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// uv: unique visitor
// 有多少用户访问过网站；pv按照userid去重
// 滑动窗口：窗口长度1小时，滑动距离5秒钟，每小时用户数量1亿
// 大数据去重的唯一解决方案：布隆过滤器
// 布隆过滤器的组成：bit数组，哈希函数
object BloomFilterGuava {

  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Long,
                          behavior: String,
                          timestamp: Long)

  def main(args: Array[String]): Unit = {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/flink-tutorial/FlinkSZ1128/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp) // 分配升序时间戳 DataStream
      .map(r => ("key", r.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new UvAggFunc,new UvProcessFunc)

    stream.print()
    env.execute()
  }

  class UvAggFunc extends AggregateFunction[(String,Long),(Long,BloomFilter[lang.Long]),Long]{
    override def createAccumulator(): (Long, BloomFilter[lang.Long]) = (0,BloomFilter.create(Funnels.longFunnel(), 100000000, 0.01))

    override def add(value: (String, Long), accumulator: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = {
      var bloom = accumulator._2
      var uvCount = accumulator._1
      if(!bloom.mightContain(value._2)){
        bloom.put(value._2)
        uvCount += 1
      }
      (uvCount,bloom)
    }

    override def getResult(accumulator: (Long, BloomFilter[lang.Long])): Long = accumulator._1

    override def merge(a: (Long, BloomFilter[lang.Long]), b: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = ???
  }
  class UvProcessFunc extends ProcessWindowFunction[Long, String, String, TimeWindow] {
    // 连接到redis
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      // 窗口结束时间 ==> UV数
      // 窗口结束时间 ==> bit数组

      // 拿到key
      val start = new Timestamp(context.window.getStart)
      val end = new Timestamp(context.window.getEnd)
      out.collect(s"窗口开始时间为$start 到 $end 的uv 为 ${elements.head}")
    }
  }
}
```

