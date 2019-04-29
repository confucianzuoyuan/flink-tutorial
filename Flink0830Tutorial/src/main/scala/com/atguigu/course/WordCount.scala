package com.atguigu.course

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {
  case class WordWithCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val text = env.socketTextStream("localhost", 9999, '\n')
    val wordCountStream = text
      .flatMap(w => w.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")

    wordCountStream.print()

    env.execute()
  }
}
