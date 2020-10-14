package com.atguigu.day10;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.*;

public class SimulatedEventSource extends RichParallelSourceFunction<MarketingUserBehavior> {
    Boolean running = true;

    ArrayList<String> channelSet = new ArrayList<>();
    ArrayList<String> behaviorTypes = new ArrayList<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        channelSet.add("XiaomiStore");
        channelSet.add("AppStore");
        behaviorTypes.add("BROWSE");
        behaviorTypes.add("CLICK");
    }

    Random rand = new Random();

    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        while (running) {
            String userId = UUID.randomUUID().toString();
            String behaviorType = behaviorTypes.get(rand.nextInt(behaviorTypes.size()));
            String channel = channelSet.get(rand.nextInt(channelSet.size()));
            long ts = Calendar.getInstance().getTimeInMillis();

            ctx.collect(new MarketingUserBehavior(userId, behaviorType, channel, ts));

            Thread.sleep(10);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

}
