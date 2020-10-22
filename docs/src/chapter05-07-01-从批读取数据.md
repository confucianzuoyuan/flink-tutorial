### 从批读取数据

```java
DataStream<String> stringStream = env
  .fromElements(
    "hello world",
    "atguigu bigdata"
  )

DataStream<Integer> intStream = env.fromElements(1, 2, 3);

DataStream<Event> stream = env
  .fromElements(
    new Event("key_1", 1, 1000L),
    new Event("key_2", 2, 2000L),
    new Event("key_3", 3, 3000L),
    new Event("key_4", 4, 4000L),
  );
```
