package com.atguigu.day2;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {
    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random rand = new Random();

        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];

        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + i;
            curFTemp[i] = 65 + (rand.nextGaussian() * 20);
        }

        while (running) {
            long curTime = Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 10; i++) {
                curFTemp[i] = rand.nextGaussian() * 0.5;
                ctx.collect(new SensorReading(sensorIds[i], curTime, curFTemp[i]));
            }

            Thread.sleep(1000L);
        }


    }

    @Override
    public void cancel() {
        running = false;
    }
}