import atguigu.entity.TemperatureEvent;
import atguigu.entity.Alert;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;

/*
* 机箱温度连续两次超过26度时，报警
* 报警信息：第二次温度超过26度时事件的机箱温度和机箱名字。
* **/
public class CEPExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TemperatureEvent> inputEventStream = env.fromElements(
                new TemperatureEvent("xyz",22.0),
                new TemperatureEvent("xyz",20.1), new TemperatureEvent("xyz",21.1),
                new TemperatureEvent("xyz",22.2), new TemperatureEvent("xyz",22.1),
                new TemperatureEvent("xyz",22.3), new TemperatureEvent("xyz",22.1),
                new TemperatureEvent("xyz",22.4), new TemperatureEvent("xyz",22.7),
                new TemperatureEvent("xyz",27.0), new TemperatureEvent("xyz", 28.0));

        // 定义Pattern，检查10秒钟内温度是否高于26度
        Pattern<TemperatureEvent,?> warningPattern = Pattern.<TemperatureEvent>begin("start")
                .subtype(TemperatureEvent.class)
                // 新建一个SimpleCondition函数, 第一个事件的匹配条件
                .where(new SimpleCondition<TemperatureEvent>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean filter(TemperatureEvent value) throws Exception {
                        if(value.getTemperature() >= 26.0){
                            return true;
                        }
                        return false;
                    }
                })
                .next("end")
                // 第二个事件的匹配条件
                .where(new SimpleCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent value) throws Exception {
                        if (value.getTemperature() >= 26.0) {
                            return true;
                        }
                        return false;
                    }
                })
                .within(Time.seconds(10));

        // 匹配pattern并select事件,符合条件的发生警告，即输出
        // Java中的[方法引用],[方法引用]的格式是类名::方法名, java8新用法
        DataStream<Alert> patternStream = CEP.pattern(inputEventStream.keyBy(TemperatureEvent::getMachineName), warningPattern)
                .select(new PatternSelectFunction<TemperatureEvent, Alert>() {
                    @Override
                    public Alert select(Map<String, List<TemperatureEvent>> pattern) throws Exception {
                        return new Alert("检测到温度升高: " + pattern.get("end").get(0).getTemperature() + " 机器名称: " + pattern.get("end").get(0).getMachineName());
                    }
                });

        patternStream.print();

        env.execute();

    }
}