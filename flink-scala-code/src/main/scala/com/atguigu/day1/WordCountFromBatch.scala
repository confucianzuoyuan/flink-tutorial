package com.atguigu.day1

// 导入一些隐式类型转换
import org.apache.flink.streaming.api.scala._

object WordCountFromBatch {

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 并行度设置为1，所有计算都在同一个分区执行
    env.setParallelism(1)

    val stream = env
      .fromElements(
        "hello word",
        "hello world"
      )
      // 使用空格分割字符串 `\\s`表示空格
      .flatMap(w => w.split("\\s"))
      // 相当于MapReduce中的Map操作
      .map(w => WordCount(w, 1))
      // shuffle操作
      .keyBy(_.word)
      // 聚合`count`字段
      .sum(1)

    stream.print()

    env.execute()
  }

  case class WordCount(word: String, count: Int)
}