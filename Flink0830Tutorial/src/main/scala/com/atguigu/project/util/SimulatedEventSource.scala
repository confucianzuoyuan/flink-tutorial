package com.atguigu.project.util

import java.util.{Calendar, UUID}

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior] {

  var running = true

  val channelSet = Seq("AppStore", "XiaomiStore")
  val behaviorTypes = Seq("BROWSE", "CLICK", "INSTALL", "UNINSTALL", "DOWNLOAD")
  val rand = scala.util.Random

  override def run(ctx: SourceContext[MarketingUserBehavior]): Unit = {
    while (running) {
      val userId = UUID.randomUUID().toString
      val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = Calendar.getInstance().getTimeInMillis

      ctx.collect(MarketingUserBehavior(userId, behaviorType, channel, ts))

      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = false
}