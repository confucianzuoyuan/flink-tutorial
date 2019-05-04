import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;

public class FirstExample {

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过列表创建数据流DataStream, 类型为Person
        DataStream<Person> flintstones = env.fromElements(
                new Person("佟老师", 35),
                new Person("宋老师", 35),
                new Person("左老师", 2));

        // 过滤算子，需要override自定义一下filter算子
        DataStream<Person> adults = flintstones.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.age >= 18;
            }
        });

        adults.print();

        env.execute();
    }

    // POJOs类, 容易优化
    public static class Person {
        public String name;
        public Integer age;
        public Person() {};

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        };

        // 打印的格式定义
        public String toString() {
            return this.name.toString() + ": age " + this.age.toString();
        };
    }
}