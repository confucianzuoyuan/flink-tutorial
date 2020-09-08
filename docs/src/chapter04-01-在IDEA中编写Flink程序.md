## 在IDEA中编写Flink程序

本项目使用的Flink版本为最新版本，也就是1.11.0。现在提供maven项目的配置文件。

1. 使用Intellij IDEA创建一个Maven新项目
2. 勾选`Create from archetype`，然后点击`Add Archetype`按钮
3. `GroupId`中输入`org.apache.flink`，`ArtifactId`中输入`flink-quickstart-scala`，`Version`中输入`1.11.0`，然后点击`OK`
4. 点击向右箭头，出现下拉列表，选中`flink-quickstart-scala:1.11.0`，点击`Next`
5. `Name`中输入`FlinkTutorial`，`GroupId`中输入`com.atguigu`，`ArtifactId`中输入`FlinkTutorial`，点击`Next`
6. 最好使用IDEA默认的Maven工具：Bundled（Maven 3），点击`Finish`，等待一会儿，项目就创建好了

编写`WordCount.scala`程序

```java
package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingJob {

  /** Main program method */
  def main(args: Array[String]) : Unit = {

    // get the execution environment
    StreamExecutionEnvironment env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    // get input data by connecting to the socket
    val text: DataStream[String] = env
      .socketTextStream("localhost", 9999, '\n')

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts
      .print()
      .setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(word: String, count: Long)
}
```

打开一个终端（Terminal），运行以下命令

```sh
$ nc -lk 9999
```

接下来使用`IDEA`运行就可以了。

