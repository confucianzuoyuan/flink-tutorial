package com.atguigu.refactorcode;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class StreamJoinMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new EventSource())
                .map(new RichMapFunction<Event, String>() {
                    private final HashMap<String, Long> map = new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        Connection conn = DriverManager.getConnection(
                                "jdbc:mysql://localhost:3306/sensor",
                                "zuoyuan",
                                "zuoyuan"
                        );

                        Statement stmt = conn.createStatement();
                        String sql;
                        sql = "SELECT key, value FROM kvtable";
                        ResultSet rs = stmt.executeQuery(sql);
                        while (rs.next()) {
                            String key = rs.getString("key");
                            long value = rs.getLong("value");
                            map.put(key, value);
                        }
                        rs.close();
                        stmt.close();
                        conn.close();
                    }

                    @Override
                    public String map(Event event) throws Exception {
                        if (map.containsKey(event.key)) {
                            return event.key;
                        } else {
                            return "MySQL表中不包含key：" + event.key;
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }
                })
                .print();

        env.execute();
    }
}
