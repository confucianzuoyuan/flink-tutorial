## Flink和Hive集成

Apache Hive 已经成为了数据仓库生态系统中的核心。 它不仅仅是一个用于大数据分析和ETL场景的SQL引擎，同样它也是一个数据管理平台，可用于发现，定义，和演化数据。

Flink 与 Hive 的集成包含两个层面。

一是利用了 Hive 的 MetaStore 作为持久化的 Catalog，用户可通过HiveCatalog将不同会话中的 Flink 元数据存储到 Hive Metastore 中。 例如，用户可以使用HiveCatalog将其 Kafka 表或 Elasticsearch 表存储在 Hive Metastore 中，并后续在 SQL 查询中重新使用它们。

二是利用 Flink 来读写 Hive 的表。

HiveCatalog的设计提供了与 Hive 良好的兼容性，用户可以”开箱即用”的访问其已有的 Hive 数仓。 您不需要修改现有的 Hive Metastore，也不需要更改表的数据位置或分区。