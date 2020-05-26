package com.atguigu.day5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinExample {

  // 用户点击日志
  case class UserClickLog(userID: String,
                          eventTime: String,
                          eventType: String,
                          pageID: String)

  // 用户浏览日志
  case class UserBrowseLog(userID: String,
                           eventTime: String,
                           eventType: String,
                           productID: String,
                           productPrice: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val clickStream = env
      .fromElements(
        UserClickLog("user_2", "1500", "click", "page_1"), // (900, 1500)
        UserClickLog("user_2", "2000", "click", "page_1")  // (1400, 2000)
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.userID)

    val browseStream = env
      .fromElements(
        UserBrowseLog("user_2", "1000", "browse", "product_1", "10"), // (1000, 1600)
        UserBrowseLog("user_2", "1500", "browse", "product_1", "10"), // (1500, 2100)
        UserBrowseLog("user_2", "1501", "browse", "product_1", "10"), // (1501, 2101)
        UserBrowseLog("user_2", "1502", "browse", "product_1", "10")  // (1502, 2102)
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.userID)

    clickStream
      .intervalJoin(browseStream)
      .between(Time.minutes(-10), Time.seconds(0))
      .process(new MyIntervalJoin)
      .print()

    env.execute()
  }

  class MyIntervalJoin extends ProcessJoinFunction[UserClickLog, UserBrowseLog, String] {
    override def processElement(left: UserClickLog, right: UserBrowseLog, ctx: ProcessJoinFunction[UserClickLog, UserBrowseLog, String]#Context, out: Collector[String]): Unit = {
      out.collect(left + " ==> " + right)
    }
  }
}