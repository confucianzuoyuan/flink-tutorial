package com.atguigu.day1

import org.apache.flink.streaming.api.scala._

object WordCountFromBatch {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境，类似SparkContext
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 并行任务的数量设置为1
    // 全局并行度
    env.setParallelism(1)

    val stream = env
      .fromElements(
        "zuoyuan",
        "hello world",
        "zuoyuan",
        "zuoyuan"
      )
      .setParallelism(1)

    // 对数据流进行转换算子操作
    val textStream = stream
      // 使用空格来进行切割输入流中的字符串
      .flatMap(r => r.split("\\s"))
      .setParallelism(2)
      // 做map操作, w => (w, 1)
      .map(w => WordWithCount(w, 1))
      .setParallelism(2)
      // 使用word字段进行分组操作，也就是shuffle
      .keyBy(0)
      // 做聚合操作，类似与reduce
      .sum(1)
        .setParallelism(2)

    // 将数据流输出到标准输出，也就是打印
    // 设置并行度为1，print算子的并行度就是1，覆盖了全局并行度
    textStream.print().setParallelism(2)

    // 不要忘记执行！
    env.execute()
  }

  case class WordWithCount(word: String, count: Int)
}
