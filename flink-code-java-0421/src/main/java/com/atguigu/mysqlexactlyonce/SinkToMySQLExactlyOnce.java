package com.atguigu.mysqlexactlyonce;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.*;

public class SinkToMySQLExactlyOnce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        SingleOutputStreamOperator<Long> stream = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Long>() {
                    @Override
                    public Long map(String value) throws Exception {
                        return Long.parseLong(value);
                    }
                });

        stream.print();

        stream.addSink(new MySQLTwoPC());

        env.execute();
    }

    public static class MySQLTwoPC extends TwoPhaseCommitSinkFunction<Long, Connection, Void> {

        public MySQLTwoPC() {
            super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
        }

        @Override
        protected Connection beginTransaction() throws Exception {
            Connection conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/sensor",
                    "zuoyuan",
                    "zuoyuan"
            );
            conn.setAutoCommit(false);
            System.out.println("tx start");
            return conn;
        }

        @Override
        protected void preCommit(Connection transaction) throws Exception {
            System.out.println("preCommit start");
        }

        @Override
        protected void invoke(Connection transaction, Long value, Context context) throws Exception {
            PreparedStatement insertStmt = transaction.prepareStatement("INSERT INTO exactly (value, insert_time) VALUES (?, ?)");
            insertStmt.setString(1, String.valueOf(value));
            insertStmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            insertStmt.execute();
            if (value == 15L) {
                System.out.println(1 / 0);
            }
        }

        @Override
        protected void commit(Connection transaction) {
            if (transaction != null) {
                try {
                    transaction.commit();
                    System.out.println("commit complete");
                } catch (SQLException e) {
                    System.out.println("commit fail");
                    e.printStackTrace();
                } finally {
                    try {
                        transaction.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        protected void abort(Connection transaction) {
            if (transaction != null) {
                try {
                    transaction.rollback();
                    System.out.println("rollback complete");
                } catch (SQLException e) {
                    System.out.println("rollback fail");
                    e.printStackTrace();
                } finally {
                    try {
                        transaction.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
