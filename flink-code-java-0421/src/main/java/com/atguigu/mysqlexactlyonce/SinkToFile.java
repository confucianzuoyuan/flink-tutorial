package com.atguigu.mysqlexactlyonce;

import javolution.io.Struct;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SinkToFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        DataStreamSource<Long> stream = env.addSource(new CountSource());

        stream.print();

        stream.addSink(new TransactionalFileSink());

        env.execute();
    }

    public static class CountSource implements SourceFunction<Long> {
        private Boolean running = true;
        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long cnt = -1;
            while (running && cnt < Long.MAX_VALUE) {
                cnt += 1;
                ctx.collect(cnt);
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class TransactionalFileSink extends TwoPhaseCommitSinkFunction<Long, String, Void> {

        private BufferedWriter transactionWriter;

        public TransactionalFileSink() {
            super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
        }

        @Override
        protected String beginTransaction() throws Exception {
            long timeNow = System.currentTimeMillis();
            int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();
            String transactionFile = timeNow + "-" + taskIdx;
            Path tFilePath = Paths.get("/home/zuoyuan/filetemp/" + transactionFile);
            Files.createFile(tFilePath);
            this.transactionWriter = Files.newBufferedWriter(tFilePath);
            System.out.println("create tx");
            return transactionFile;
        }

        @Override
        protected void invoke(String transaction, Long value, Context context) throws Exception {
            transactionWriter.write(value.toString());
            transactionWriter.write('\n');
        }

        @Override
        protected void preCommit(String transaction) throws Exception {
            transactionWriter.flush();
            transactionWriter.close();
        }

        @Override
        protected void commit(String transaction) {
            Path tFilePath = Paths.get("/home/zuoyuan/filetemp/" + transaction);
            if (Files.exists(tFilePath)) {
                try {
                    Path cFilePath = Paths.get("/home/zuoyuan/filetarget/" + transaction);
                    Files.move(tFilePath, cFilePath);
                    System.out.println("commit complete");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        protected void abort(String transaction) {
            Path tFilePath = Paths.get("/home/zuoyuan/filetemp/" + transaction);
            if (Files.exists(tFilePath)) {
                try {
                    Files.delete(tFilePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
