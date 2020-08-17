package com.atguigu.proj

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.module.hive.HiveModule

import scala.collection.mutable.ListBuffer
import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object TestHiveStreaming {

  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().inBatchMode().build()
    val tableEnv = TableEnvironment.create(settings)

    val name            = "myhive"
    val defaultDatabase = "usertags"
    val hiveConfDir     = "/Users/yuanzuo/Downloads/apache-hive-3.1.2-bin/conf" // a local path
    val version         = "3.1.2"

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnv.registerCatalog("myhive", hive)

    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog("myhive")
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    tableEnv.useDatabase("usertags")

    tableEnv.loadModule(name, new HiveModule(version))

    val member = "select id as memberId,phone,sex,member_channel as channel,mp_open_id as subOpenId," +
      " address_default_id as address,date_format(create_time,'yyyy-MM-dd') as regTime" +
      " from usertags.t_member"

    val order_commodity = "select o.member_id as memberId," +
      " date_format(max(o.create_time),'yyyy-MM-dd') as orderTime," +
      " count(o.order_id) as orderCount," +
      " collect_list(DISTINCT oc.commodity_id) as favGoods, " +
      " sum(o.pay_price) as orderMoney " +
      " from usertags.t_order as o left join usertags.t_order_commodity as oc" +
      " on o.order_id = oc.order_id group by o.member_id"

    val freeCoupon = "select member_id as memberId, " +
      " date_format(create_time,'yyyy-MM-dd') as freeCouponTime " +
      " from usertags.t_coupon_member where coupon_id = 1"

    val couponTimes = "select member_id as memberId," +
      " collect_list(date_format(create_time,'yyyy-MM-dd')) as couponTimes" +
      "  from usertags.t_coupon_member where coupon_id <> 1 group by member_id"

    val chargeMoney = "select cm.member_id as memberId , sum(c.coupon_price/2) as chargeMoney " +
      " from usertags.t_coupon_member as cm left join usertags.t_coupon as c " +
      " on cm.coupon_id = c.id where cm.coupon_channel = 1 group by cm.member_id"

    val overTime = "select (to_unix_timestamp(max(arrive_time)) - to_unix_timestamp(max(pick_time))) " +
      " as overTime, member_id as memberId " +
      " from usertags.t_delivery group by member_id"

    val feedback = "select fb.feedback_type as feedback,fb.member_id as memberId" +
      " from usertags.t_feedback as fb " +
      " left join (select max(id) as mid,member_id as memberId " +
      " from usertags.t_feedback group by member_id) as t " +
      " on fb.id = t.mid"

    val resultSQL = "select m.*,o.orderCount,o.orderTime,o.orderMoney,o.favGoods," +
      " fb.freeCouponTime,ct.couponTimes, cm.chargeMoney,ot.overTime,f.feedback" +
      " from (" + member + ") as m " +
      " left join (" + order_commodity + ") as o on m.memberId = o.memberId " +
      " left join (" + freeCoupon + ") as fb on m.memberId = fb.memberId " +
      " left join (" + couponTimes + ") as ct on m.memberId = ct.memberId " +
      " left join (" + chargeMoney + ") as cm on m.memberId = cm.memberId " +
      " left join (" + overTime + ") as ot on m.memberId = ot.memberId " +
      " left join (" + feedback + ") as f on m.memberId = f.memberId "

    val result = tableEnv.sqlQuery(resultSQL)

    val fields = result.getSchema.getFieldNames

    val res = result.execute().collect()

    val rowList = new ListBuffer[util.HashMap[String, String]]()

    while (res.hasNext) {
      val row = res.next()
      val r = new util.HashMap[String, String]()

      for (field <- fields) {
        val idx = fields.indexOf(field)
        if (row.getField(idx) != null) {
          if (fields(idx).equals("couponTimes")) {
            r.put(fields(idx), row.getField(idx).asInstanceOf[Array[String]].mkString(","))
          } else if (fields(idx).equals("favGoods")) {
            r.put(fields(idx), row.getField(idx).asInstanceOf[Array[Integer]].mkString(","))
          } else {
            r.put(fields(idx), row.getField(idx).toString)
          }

        }
      }
      rowList += r
    }


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 有关es的配置的样板代码
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))
    val esSinkBuilder = new ElasticsearchSink.Builder[util.HashMap[String, String]](
      httpHosts,
      new ElasticsearchSinkFunction[util.HashMap[String, String]] {
        override def process(t: util.HashMap[String, String], runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          val indexRequest = Requests
            .indexRequest()
            .index("tag") // 索引是sensor，相当于数据库
            .source(t)

          requestIndexer.add(indexRequest)
        }
      }
    )

    // 设置每一批写入es多少数据
    esSinkBuilder.setBulkFlushMaxActions(1)

    val stream = env.fromCollection(rowList)
    stream.print()
    stream.addSink(esSinkBuilder.build())

    env.execute()
  }
}