## 执行

当应用程序完全写好时，我们可以调用`StreamExecutionEnvironment.execute()`来执行应用程序。在我们的例子中就是我们的最后一行调用：

```
env.execute("Compute average sensor temperature")
```

Flink程序是惰性执行的。也就是说创建数据源和转换算子的API调用并不会立刻触发任何数据处理逻辑。API调用仅仅是在执行环境中构建了一个执行计划，这个执行计划包含了执行环境创建的数据源和所有的将要用在数据源上的转换算子。只有当`execute()`被调用时，系统才会触发程序的执行。

构建好的执行计划将被翻译成一个`JobGraph`并提交到`JobManager`上面去执行。根据执行环境的种类，一个`JobManager`将会运行在一个本地线程中（如果是本地执行环境的化）或者`JobGraph`将会被发送到一个远程的`JobManager`上面去。如果`JobManager`远程运行，那么`JobGraph`必须和一个包含有所有类和应用程序的依赖的JAR包一起发送到远程`JobManager`。

