### 彻底重置hadoop和hive的方法

```sh
stop-all.sh
hadoop namenode -format
# 在mysql中删除hive的元数据库
start-all.sh
hadoop fs -mkdir /tmp
hadoop fs -mkdir -p /user/hive/warehouse
hadoop fs -chmod g+w /tmp
hadoop fs -chmod g+w /user/hive/warehouse
schematool -dbType mysql -initSchema
hive --service metastore
hive
```