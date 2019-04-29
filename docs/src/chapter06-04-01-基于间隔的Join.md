### 基于间隔的Join

基于间隔的Join会对两条流中拥有相同键值以及彼此之间时间戳不超过某一指定间隔的事件进行Join。

下图展示了两条流（A和B）上基于间隔的Join，如果B中事件的时间戳相较于A中事件的时间戳不早于1小时且不晚于15分钟，则会将两个事件Join起来。Join间隔具有对称性，因此上面的条件也可以表示为A中事件的时间戳相较B中事件的时间戳不早于15分钟且不晚于1小时。

![](images/spaf_0607.png)

基于间隔的Join目前只支持事件时间以及INNER JOIN语义（无法发出未匹配成功的事件）。下面的例子定义了一个基于间隔的Join。

```scala
input1
  .keyBy(...)
  .between(<lower-bound>, <upper-bound>) // 相对于input1的上下界
  .process(ProcessJoinFunction) // 处理匹配的事件对
```

Join成功的事件对会发送给ProcessJoinFunction。下界和上界分别由负时间间隔和正时间间隔来定义，例如between(Time.hour(-1), Time.minute(15))。在满足下界值小于上界值的前提下，你可以任意对它们赋值。例如，允许出现B中事件的时间戳相较A中事件的时间戳早1～2小时这样的条件。

基于间隔的Join需要同时对双流的记录进行缓冲。对第一个输入而言，所有时间戳大于当前水位线减去间隔上界的数据都会被缓冲起来；对第二个输入而言，所有时间戳大于当前水位线加上间隔下界的数据都会被缓冲起来。注意，两侧边界值都有可能为负。上图中的Join需要存储数据流A中所有时间戳大于当前水位线减去15分钟的记录，以及数据流B中所有时间戳大于当前水位线减去1小时的记录。不难想象，如果两条流的事件时间不同步，那么Join所需的存储就会显著增加，因为水位线总是由“较慢”的那条流来决定。

例子：每个用户的点击Join这个用户最近10分钟内的浏览

```scala
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

// 需求：每个用户的点击Join这个用户最近10分钟内的浏览

// 数据流clickStream
// 某个用户在某个时刻点击了某个页面
// {"userID": "user_2", "eventTime": "2019-11-16 17:30:02", "eventType": "click", "pageID": "page_1"}

// 数据流browseStream
// 某个用户在某个时刻浏览了某个商品，以及商品的价值
// {"userID": "user_2", "eventTime": "2019-11-16 17:30:01", "eventType": "browse", "productID": "product_1", "productPrice": 10}
object IntervalJoinExample {

  case class UserClickLog(userID: String,
                          eventTime: String,
                          eventType: String,
                          pageID: String)

  case class UserBrowseLog(userID: String,
                           eventTime: String,
                           eventType: String,
                           productID: String,
                           productPrice: String)

  def main(args: Array[String]): Unit = {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val clickStream = env
      .fromElements(
        UserClickLog("user_2", "2019-11-16 17:30:00", "click", "page_1")
      )
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[UserClickLog](Time.seconds(0)) {
          override def extractTimestamp(t: UserClickLog): Long = {
            val dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
            val dateTime = DateTime.parse(t.eventTime, dateTimeFormatter)
            dateTime.getMillis
          }
        }
      )

    val browseStream = env
      .fromElements(
        UserBrowseLog("user_2", "2019-11-16 17:19:00", "browse", "product_1", "10"),
        UserBrowseLog("user_2", "2019-11-16 17:20:00", "browse", "product_1", "10"),
        UserBrowseLog("user_2", "2019-11-16 17:22:00", "browse", "product_1", "10"),
        UserBrowseLog("user_2", "2019-11-16 17:26:00", "browse", "product_1", "10"),
        UserBrowseLog("user_2", "2019-11-16 17:30:00", "browse", "product_1", "10"),
        UserBrowseLog("user_2", "2019-11-16 17:31:00", "browse", "product_1", "10")
      )
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[UserBrowseLog](Time.seconds(0)) {
          override def extractTimestamp(t: UserBrowseLog): Long = {
            val dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
            val dateTime = DateTime.parse(t.eventTime, dateTimeFormatter)
            dateTime.getMillis
          }
        }
      )

    clickStream
      .keyBy("userID")
      .intervalJoin(browseStream.keyBy("userID"))
      .between(Time.minutes(-10),Time.seconds(0))
      .process(new MyIntervalJoin)
      .print()

    env.execute()
  }

  class MyIntervalJoin extends ProcessJoinFunction[UserClickLog, UserBrowseLog, String] {
    override def processElement(
      left: UserClickLog,
      right: UserBrowseLog,
      context: ProcessJoinFunction[UserClickLog, UserBrowseLog, String]#Context,
      out: Collector[String]
    ): Unit = {
      out.collect(left +" =Interval Join=> "+right)
    }
  }
}
```

