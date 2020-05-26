package com.atguigu.day1

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCountFromSocket {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境，类似SparkContext
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 并行任务的数量设置为1
    env.setParallelism(1)

    // 数据源来自socket端口
    // 本地是localhost，如果是虚拟机的话，可能是`hadoop102`之类的
    // 本地启动一个`nc -lk 9999`
    // 你们可能需要在hadoop102的终端启动`nc -lk 9999`
    val stream = env.socketTextStream("localhost", 9999, '\n')

    // 对数据流进行转换算子操作
    val textStream = stream
      // 使用空格来进行切割输入流中的字符串
      .flatMap(r => r.split("\\s"))
      // 做map操作, w => (w, 1)
      .map(w => WordWithCount(w, 1))
      // 使用word字段进行分组操作，也就是shuffle
      .keyBy(0)
      // 分流后的每一条流上，开5s的滚动窗口
      .timeWindow(Time.seconds(5))
      // 做聚合操作，类似与reduce
      .sum(1)

    // 将数据流输出到标准输出，也就是打印
    textStream.print()

    // 不要忘记执行！
    env.execute()
  }

  case class WordWithCount(word: String, count: Int)
}
