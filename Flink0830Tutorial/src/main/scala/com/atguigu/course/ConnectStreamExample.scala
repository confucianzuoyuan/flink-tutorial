package com.atguigu.course

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object ConnectStreamExample {
  // DataStream -> ConnectedStream -> DataStream
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream1: DataStream[(Int, String)] = env.fromElements(
      (1, "a"),
      (2, "b")
    )
    val stream2: DataStream[(Int, Int)] = env.fromElements(
      (1, 1),
      (1, 2)
    )

    // select * from A inner join B on A.id = B.id;
    // conn和conn1的写法是等价的
    val conn: ConnectedStreams[(Int, String), (Int, Int)] = stream1
      .keyBy(_._1)
      .connect(stream2.keyBy(_._1))

    val conn1: ConnectedStreams[(Int, String), (Int, Int)] = stream1
      .connect(stream2)
      .keyBy(0,0)

    val outStream: DataStream[String] = conn.map(new MyCoMapFunction)

    outStream.print()

    env.execute()
  }

  class MyCoMapFunction extends CoMapFunction[(Int, String), (Int, Int), String] {
    override def map1(value: (Int, String)): String = value._2 + " from map1"

    override def map2(value: (Int, Int)): String = value._2.toString + " from map2"
  }
}
