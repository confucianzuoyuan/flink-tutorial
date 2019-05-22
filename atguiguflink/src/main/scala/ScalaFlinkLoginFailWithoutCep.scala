import java.util

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.TimeCharacteristic

//case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: String)

object ScalaFlinkLoginFailWithoutCep {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventStream = env.fromCollection(List(
      LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
      LoginEvent("1", "192.168.0.2", "fail", "1558430843"),
      LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
      LoginEvent("2", "192.168.10.10", "success", "1558430845")
    ))

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
      new ListStateDescriptor[LoginEvent]("saved ride", classOf[LoginEvent]))

    override def processElement(ride: LoginEvent,
                                context: KeyedProcessFunction[String, LoginEvent, LoginEvent]#Context,
                                out: Collector[LoginEvent]): Unit = {
      val timerService = context.timerService

      if (ride.eventType == "fail") {
        loginState.add(ride)
      }

      timerService.registerEventTimeTimer(ride.eventTime.toLong + 10 * 1000)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, LoginEvent, LoginEvent]#OnTimerContext,
                         out: Collector[LoginEvent]): Unit = {
      val savedRide = loginState

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