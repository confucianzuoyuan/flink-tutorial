package com.atguigu.course

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
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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
    override def processElement(left: UserClickLog,
                                right: UserBrowseLog,
                                context: ProcessJoinFunction[UserClickLog, UserBrowseLog, String]#Context,
                                out: Collector[String]): Unit = {
      out.collect(left +" =Interval Join=> "+right)
    }
  }
}