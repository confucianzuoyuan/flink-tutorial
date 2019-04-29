### 两种planner（old & blink）的区别

1. 批流统一：Blink将批处理作业，视为流式处理的特殊情况。所以，blink不支持表和DataSet之间的转换，批处理作业将不转换为DataSet应用程序，而是跟流处理一样，转换为DataStream程序来处理。
2. 因为批流统一，Blink planner也不支持BatchTableSource，而使用有界的StreamTableSource代替。
3. Blink planner只支持全新的目录，不支持已弃用的ExternalCatalog。
4. 旧planner和Blink planner的FilterableTableSource实现不兼容。旧的planner会把PlannerExpressions下推到filterableTableSource中，而blink planner则会把Expressions下推。
5. 基于字符串的键值配置选项仅适用于Blink planner。
6. PlannerConfig在两个planner中的实现不同。
7. Blink planner会将多个sink优化在一个DAG中（仅在TableEnvironment上受支持，而在StreamTableEnvironment上不受支持）。而旧planner的优化总是将每一个sink放在一个新的DAG中，其中所有DAG彼此独立。
8. 旧的planner不支持目录统计，而Blink planner支持。

