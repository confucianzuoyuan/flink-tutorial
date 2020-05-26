### 抛弃迟到元素

抛弃迟到的元素是event time window operator的默认行为。也就是说一个迟到的元素不会创建一个新的窗口。

process function可以通过比较迟到元素的时间戳和当前水位线的大小来很轻易的过滤掉迟到元素。

