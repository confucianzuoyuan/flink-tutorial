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
                        Tuple2.of("20", "bigdata"),
                        Tuple2.of("21", "atguigu")
                );

        String name            = "myhive"; // 命名空间或者叫catalog目录
        String defaultDatabase = "mydb"; // hive中的数据库的名字
        String hiveConfDir     = "/home/zuoyuan/apache-hive-3.1.2-bin/conf"; // hive配置文件的路径
        String version         = "3.1.2"; // hive的版本

        // 创建一个catalog
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        // 注册一个catalog
        tableEnv.registerCatalog("myhive", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        // 指定sql的语法是hive sql
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        // use mydb;
        tableEnv.useDatabase("mydb");

        // 创建临时表
        tableEnv.createTemporaryView("users", stream, $("id"), $("name"));

        // 执行hive sql
        tableEnv.executeSql("insert into t_user select id, name from users");
        tableEnv.executeSql("select * from t_user");
    }
}
