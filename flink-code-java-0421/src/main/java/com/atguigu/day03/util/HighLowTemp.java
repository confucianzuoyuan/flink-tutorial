package com.atguigu.day03.util;

import java.sql.Timestamp;

// POJO class
public class HighLowTemp {
    public String id;
    public Double high, low;
    public Long windowStart, windowEnd;

    public HighLowTemp() {
    }

    public HighLowTemp(String id, Double high, Double low, Long windowStart, Long windowEnd) {
        this.id = id;
        this.high = high;
        this.low = low;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "HighLowTemp{" +
                "id='" + id + '\'' +
                ", high=" + high +
                ", low=" + low +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
