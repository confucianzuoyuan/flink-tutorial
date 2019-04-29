package com.atguigu.day3;

public class MinMaxTemp {
    public String id;
    public Double minTemp;
    public Double maxTemp;
    public String window;

    public MinMaxTemp() {}

    public MinMaxTemp(String id, Double minTemp, Double maxTemp, String window) {
        this.id = id;
        this.minTemp = minTemp;
        this.maxTemp = maxTemp;
        this.window = window;
    }

    @Override
    public String toString() {
        return "MinMaxTemp{" +
                "id='" + id + '\'' +
                ", minTemp=" + minTemp +
                ", maxTemp=" + maxTemp +
                ", window='" + window + '\'' +
                '}';
    }
}
