package com.atguigu.day10;

public class MarketingUserBehavior {
    //case class MarketingUserBehavior(userId: String,
    //                                   behavior: String,
    //                                   channel: String,
    //                                   ts: Long)
    public String userId;
    public String behavior;
    public String channel;
    public Long ts;

    public MarketingUserBehavior() {
    }

    public MarketingUserBehavior(String userId, String behavior, String channel, Long ts) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "MarketingUserBehavior{" +
                "userId='" + userId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", channel='" + channel + '\'' +
                ", ts=" + ts +
                '}';
    }
}
