package com.atguigu.day1

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCountFromSocket {

  case class WordWithCount(word: String, count: Int)

  def main(args: Array[String]): Unit = {
    // 获取运行时环境，类似SparkContext
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置分区（又叫并行任务）的数量为1
    // 这句代码会像资源管理器申请一个任务插槽`task slot`
    env.setParallelism(1)

    // 建立数据源
    // 需要先启动`nc -lk 9999`，用来发送数据
    // source操作
    val stream = env.socketTextStream("localhost", 9999, '\n')

    // 写对流的转换处理逻辑
    // 转换操作
    val transformed = stream
      // 使用空格切分输入的字符串
      .flatMap(line => line.split("\\s"))
      // 类似MR中的map
      .map(w => WordWithCount(w, 1))
      // 使用word字段进行分组，shuffle
      .keyBy(0)
      // 开了一个5s钟的滚动窗口
      .timeWindow(Time.seconds(5))
      // 针对count字段进行累加操作，类似MR中的reduce
      .sum(1)

    // 将计算的结果输出到标准输出
    // sink操作
    transformed.print()

    // 执行计算逻辑，一定要写着一句！
    env.execute()
  }
}
