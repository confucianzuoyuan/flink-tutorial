package com.atguigu.day09;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import static org.apache.flink.table.api.Expressions.$;

public class TestHiveStreaming {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple2<String, String>> stream = env
                .fromElements(
                        Tuple2.of("10", "haha"),
                        Tuple2.of("11", "hehe")
                );

        String name            = "myhive";
        String defaultDatabase = "mydb";
        String hiveConfDir     = "/home/zuoyuan/apache-hive-3.1.2-bin/conf"; // a local path
        String version         = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("mydb");

        tableEnv.createTemporaryView("users", stream, $("id"), $("name"));

        tableEnv.executeSql("insert into t_user select id, name from users");
        tableEnv.executeSql("select * from t_user");
    }
}
