## flink实现kafka2iceberg
### 1. kafka数据样例
```json
{"id":"1001","name":"RISON","age":23,"sex":"男","ts":1648610308636}
{"id":"1002","name":"ZHANGSAN","age":26,"sex":"男","ts":1648610308636}
{"id":"1003","name":"WANGFANG","age":29,"sex":"女","ts":1648610308636}
```



### 2. flink 执行脚本

``````shell
/usr/local/flink/bin/flink run \
-c com.rison.iceberg.flink.write.kafka.Kafka2Iceberg \
-m yarn-cluster \
/root/flink-dir/original-flink-iceberg-1.0-SNAPSHOT.jar
``````

![image-20220330111314952](..\pic\image-20220330111314952.png)

### 3. kafka 操作脚本

#### 3.1 创建kafka topic

````shell
# 创建topic
/usr/hdp/2.2.0.0-2041/kafka/bin/kafka-topics.sh --create \
--zookeeper tbds-172-16-16-91:2181,tbds-172-16-16-41:2181,tbds-172-16-16-67:2181 \
--replication-factor 1 \
--partitions 6 \
--topic kafka_iceberg_topic

# 查看topic
/usr/hdp/2.2.0.0-2041/kafka/bin/kafka-topics.sh --list \
--zookeeper tbds-172-16-16-91:2181,tbds-172-16-16-41:2181,tbds-172-16-16-67:2181

````

![image-20220330105924579](..\pic\image-20220330105924579.png)

#### 3.2 手动打数据

``````shell
# SASL_PLAINTEXT 认证
vim /tmp/kafka_client_sasl.conf

security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN

# 生产数据
/usr/hdp/2.2.0.0-2041/kafka/bin/kafka-console-producer.sh \
--bootstrap-server tbds-172-16-16-144:6669,tbds-172-16-16-41:6669,tbds-172-16-16-91:6669 \
--topic kafka_iceberg_topic \
--producer.config /tmp/kafka_client_sasl.conf
``````

![image-20220330125953552](..\pic\image-20220330125953552.png)

### 4. 查看表数据

``````sql
-- Submit the flink job in streaming mode for current session.
SET execution.type = streaming ;

-- Enable this switch because streaming read SQL will provide few job options in flink SQL hint options.
SET table.dynamic-table-options.enabled=true;

-- Read all the records from the iceberg current snapshot, and then read incremental data starting from that snapshot.
select * from iceberg_db.iceberg_table_kafka /*+ OPTIONS('streaming'='true', 'monitor-interval'='1')*/ ;

``````

![image-20220330130022404](..\pic\image-20220330130022404.png)

