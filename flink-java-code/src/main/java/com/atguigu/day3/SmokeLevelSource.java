package com.atguigu.day3;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SmokeLevelSource implements SourceFunction<SmokeLevel> {
    private boolean running = true;

    @Override
    public void run(SourceContext<SmokeLevel> ctx) throws Exception {
        Random rand = new Random();

        while (running) {
            if (rand.nextGaussian() > 0.8) {
                ctx.collect(SmokeLevel.HIGH);
            } else {
                ctx.collect(SmokeLevel.LOW);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}