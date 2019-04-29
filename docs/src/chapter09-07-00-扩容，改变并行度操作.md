## 扩容，改变并行度操作

```sh
$ ./bin/flink modify <jobId> -p <newParallelism>
```

例子

```sh
$ ./bin/flink modify bc0b2ad61ecd4a615d92ce25390f61ad -p 16
Modify job bc0b2ad61ecd4a615d92ce25390f61ad.
​Rescaled job bc0b2ad61ecd4a615d92ce25390f61ad. Its new parallelism is 16.
```

