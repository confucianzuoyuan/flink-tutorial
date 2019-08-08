import java.util

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.TimeCharacteristic

object ScalaFlinkLoginFailWithoutCep {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventStream = env
      .socketTextStream("localhost", 5555, '\n')
      .map(line => {
        val splitted = line.split("\\s")
        LoginEvent(splitted(0), splitted(1), splitted(2), splitted(3))
      })

    val loginFailDataStream = loginEventStream
        .assignAscendingTimestamps(_.eventTime.toLong * 1000)
        .keyBy(_.userId)
        .process(new MatchFunction())
        .print()

    env.execute
  }

  class MatchFunction extends KeyedProcessFunction[String, LoginEvent, LoginEvent] {
    // keyed, managed state
    // holds an END event if the ride has ended, otherwise a START event
    lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("saved login state", classOf[LoginEvent]))

    override def processElement(login: LoginEvent,
                                context: KeyedProcessFunction[String, LoginEvent, LoginEvent]#Context,
                                out: Collector[LoginEvent]): Unit = {

      if (login.eventType == "fail") {
        loginState.add(login)
      }

      context.timerService.registerEventTimeTimer(login.eventTime.toLong * 1000 + 10 * 1000)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, LoginEvent, LoginEvent]#OnTimerContext,
                         out: Collector[LoginEvent]): Unit = {

      println("定时器被触发！")

      var allItems: util.List[LoginEvent] = new util.ArrayList[LoginEvent]
      import scala.collection.JavaConversions._
      for (item <- loginState.get) {
        allItems.add(item)
      }

      if (allItems.length > 1) {
        out.collect(allItems(0))
      }

      loginState.clear()

    }
  }

}