import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.function.AllWindowFunction

object UserBehaviorUv {

  def main(args: Array[String]): Unit = {
    val resourcesPath = getClass.getResource("/UserBehavior.csv")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      //      .readTextFile(resourcesPath.getPath)
      //      .map(line => {
      //        val linearray = line.split(",")
      //        UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
      //      })
      .fromCollection(List(
      UserBehavior(1, 1, 1, "1", 1561107112),
      UserBehavior(1, 1, 1, "1", 1561107113),
      UserBehavior(1, 1, 1, "1", 1561107114),
      UserBehavior(1, 1, 1, "1", 1561107115),
      UserBehavior(1, 1, 1, "1", 1561107116),
      UserBehavior(1, 1, 1, "1", 1561107117)
    ))
      .assignAscendingTimestamps(_.timestamp * 1000)
      //      .filter(_.behavior.equals("pv"))
      .map(_ => 1)
      //      .timeWindowAll(Time.minutes(1))
      .timeWindowAll(Time.seconds(4))
      .sum(0)
      .print()


    env.execute("Hot Items Job")
  }

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L
    override def add(userBehavior: UserBehavior, acc: Long): Long = acc + 1
    override def getResult(acc: Long): Long = acc
    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }

}