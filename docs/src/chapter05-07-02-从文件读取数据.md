### 从文件读取数据

**scala version**

```scala
val stream = env.readTextFile(filePath)
```

**java version**

```java
DataStream<String> stream = env.readTextFile(filePath);
```