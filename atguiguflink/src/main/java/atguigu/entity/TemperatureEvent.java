package atguigu.entity;

public class TemperatureEvent extends MonitoringEvent{

    public TemperatureEvent(String machineName) {
        super(machineName);
    }

    private double temperature;

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        long temp;
        temp = Double.doubleToLongBits(temperature);
        result = (int) (prime * result +(temp ^ (temp >>> 32)));

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) return true;
        if(!super.equals(obj)) return false;
        if(getClass() != obj.getClass()) return false;

        TemperatureEvent other = (TemperatureEvent) obj;
        if(Double.doubleToLongBits(temperature) != Double.doubleToLongBits(other.temperature)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "TemperatureEvent [getTemperature()=" + getTemperature() + ", getMachineName=" + getTemperature() + "]";
    }

    public TemperatureEvent(String machineName, double temperature) {
        super(machineName);
        this.temperature = temperature;
    }
}