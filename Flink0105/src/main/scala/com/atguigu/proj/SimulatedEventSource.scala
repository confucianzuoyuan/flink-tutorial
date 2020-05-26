package com.atguigu.proj

import java.util.{Calendar, UUID}

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehaviour] {
  var running = true

  val channelSet = Seq("AppStore", "XiaomiStore", "HuaweiStore")
  val behaviourTypes = Seq("BROWSE", "CLICK", "INSTALL", "UNINSTALL")

  val rand = new Random

  override def run(ctx: SourceContext[MarketingUserBehaviour]): Unit = {
    while (running) {
      val userId = UUID.randomUUID().toString
      val behaviourType = behaviourTypes(rand.nextInt(behaviourTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = Calendar.getInstance().getTimeInMillis

      ctx.collect(MarketingUserBehaviour(userId, behaviourType, channel, ts))
      Thread.sleep(10)
    }
  }

  override def cancel(): Unit = running = false
}