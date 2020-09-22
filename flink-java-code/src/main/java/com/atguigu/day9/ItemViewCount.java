package com.atguigu.day9;

import java.sql.Timestamp;

public class ItemViewCount {
    public String itemId;
    public Long windowEnd;
    public Long count;

    public ItemViewCount(String itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public ItemViewCount() {
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId='" + itemId + '\'' +
                ", windowEnd=" + new Timestamp(windowEnd) +
                ", count=" + count +
                '}';
    }
}
