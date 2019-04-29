package com.atguigu.course

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfo}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row

object TableUDFExample3 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val myAggFunc = new MyMinMax
    env.setParallelism(1)
    val stream = env.fromElements(
      (1, -1),
      (1, 2)
    )
    val table = tEnv.fromDataStream(stream, 'key, 'a)
    table
      .groupBy('key)
      .aggregate(myAggFunc('a) as ('x, 'y))
      .select('key, 'x, 'y)
      .toRetractStream[(Long, Long, Long)]
      .print()

    env.execute()
  }

  case class MyMinMaxAcc(var min: Int, var max: Int)

  class MyMinMax extends AggregateFunction[Row, MyMinMaxAcc] {

    def accumulate(acc: MyMinMaxAcc, value: Int): Unit = {
      if (value < acc.min) {
        acc.min = value
      }
      if (value > acc.max) {
        acc.max = value
      }
    }

    override def createAccumulator(): MyMinMaxAcc = MyMinMaxAcc(0, 0)

    def resetAccumulator(acc: MyMinMaxAcc): Unit = {
      acc.min = 0
      acc.max = 0
    }

    override def getValue(acc: MyMinMaxAcc): Row = {
      Row.of(Integer.valueOf(acc.min), Integer.valueOf(acc.max))
    }

    override def getResultType: TypeInformation[Row] = {
      new RowTypeInfo(Types.INT, Types.INT)
    }
  }
}