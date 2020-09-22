### 一个复杂一点的程序

```java
import java.sql.Timestamp

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

object TestHiveStreaming {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val stream = env.fromElements(
      ("1", 1000, new Timestamp(1000L)),
      ("2", 2000, new Timestamp(2000L)),
      ("3", 3000, new Timestamp(3000L))
    )

    val name            = "myhive"
    val defaultDatabase = "mydb"
    val hiveConfDir     = "/Users/yuanzuo/Downloads/apache-hive-3.1.2-bin/conf" // a local path
    val version         = "3.1.2"

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnv.registerCatalog("myhive", hive)

    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog("myhive")
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    tableEnv.useDatabase("mydb")

    tableEnv.createTemporaryView("users", stream, 'userId, 'amount, 'ts)

    val hiveSql = "CREATE external TABLE fs_table (\n" +
                     "  user_id STRING,\n" +
                     "  order_amount DOUBLE" +
                     ") partitioned by (dt string,h string,m string) " +
                     "stored as ORC " +
                     "TBLPROPERTIES (\n" +
                     "  'partition.time-extractor.timestamp-pattern'='$dt $h:$m:00',\n" +
                     "  'sink.partition-commit.delay'='0s',\n" +
                     "  'sink.partition-commit.trigger'='partition-time',\n" +
                     "  'sink.partition-commit.policy.kind'='metastore'" +
                     ")"

    tableEnv.executeSql(hiveSql)

    val insertSql = "insert into fs_table SELECT userId, amount, " +
      " DATE_FORMAT(ts, 'yyyy-MM-dd'), DATE_FORMAT(ts, 'HH'), DATE_FORMAT(ts, 'mm') FROM users"
    tableEnv.executeSql(insertSql)
  }
}
```
