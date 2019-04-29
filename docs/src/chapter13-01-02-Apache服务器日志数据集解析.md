### Apache服务器日志数据集解析

这里以apache服务器的一份log为例，每一行日志记录了访问者的IP、userId、访问时间、访问方法以及访问的url，具体描述如下：

| 字段名    | 数据类型 | 说明                         |
|-----------|----------|------------------------------|
| ip        | String   | 访问的IP                     |
| userId    | Long     | 访问的userId                 |
| eventTime | Long     | 访问时间                     |
| method    | String   | 访问方法 GET/POST/PUT/DELETE |
| url       | String   | 访问的url                    |

