package com.atguigu.refactorcode;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import java.util.Calendar;
import java.util.Random;

public class EventSource extends RichParallelSourceFunction<Event> {

    private boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random rand = new Random();

        while (running) {
            long curTime = Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 3; i++) {
                long curValue = rand.nextLong();
                ctx.collect(new Event("key_" + (i + 1), curValue, curTime));
            }
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
