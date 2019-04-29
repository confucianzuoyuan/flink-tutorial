package com.atguigu.project.loginfail

import com.atguigu.project.util.LoginEvent
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFailWithoutCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        LoginEvent("1", "0.0.0.0", "fail", "1"),
//        LoginEvent("1", "0.0.0.0", "success", "2"),
        LoginEvent("1", "0.0.0.0", "fail", "3"),
        LoginEvent("1", "0.0.0.0", "fail", "4")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.userId)
      .process(new MatchFunction)

    stream.print()
    env.execute()
  }

  class MatchFunction extends KeyedProcessFunction[String, LoginEvent, String] {
    lazy val loginState = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("login-fail", Types.of[LoginEvent])
    )

    lazy val timestamp = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("ts", Types.of[Long])
    )

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[String, LoginEvent, String]#Context, out: Collector[String]): Unit = {
      if (value.eventType == "fail") {
        loginState.add(value)
        if (timestamp.value() == 0L) {
          val ts = value.eventTime.toLong * 1000 + 5000L
          timestamp.update(ts)
          ctx.timerService().registerEventTimeTimer(ts)
        }
      }

      if (value.eventType == "success") {
        loginState.clear()
        ctx.timerService().deleteEventTimeTimer(timestamp.value())
        timestamp.clear()
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, LoginEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allLogins = ListBuffer[LoginEvent]()

      import scala.collection.JavaConversions._

      for (login <- loginState.get) {
        allLogins += login
      }

      loginState.clear()

      if (allLogins.length > 2) {
        out.collect("5s以内连续三次登录失败！")
      }
    }
  }
}