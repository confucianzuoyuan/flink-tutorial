### 自定义数据源

```java
public class EventSource extends RichParallelSourceFunction<EventReading> {

    private boolean running = true;

    @Override
    public void run(SourceContext<EventReading> ctx) throws Exception {
        Random rand = new Random();

        while (running) {
            long curTime = Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 3; i++) {
                long curValue = rand.nextLong();
                ctx.collect(new Event("key_" + (i + 1), curTime, curValue));
            }
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
```

使用方法

```java
// 摄入数据流
DataStream<Event> eventStream = env.addSource(new EventSource());
```
