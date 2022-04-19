package com.rison.iceberg.flink.cdc.mysql;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PACKAGE_NAME: com.rison.iceberg.flink.cdc.mysql
 * @NAME: Mysql2Kafka
 * @USER: Rison
 * @DATE: 2022/4/20 0:56
 * @PROJECT_NAME: iceberg
 **/
public class Mysql2Kafka {
    private static String CHECKPOINT_DATA_URI = "hdfs:///flink/checkpoints-data/";

    public static void main(String[] args) throws Exception {
        //TODO 1. set flink env and set checkpoint
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(1_000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(1_1000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        checkpointConfig.setCheckpointTimeout(60_1000L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend(CHECKPOINT_DATA_URI));
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.setParallelism(1);
        // 创建表，connector使用mysql-cdc
        String mysqlCDCSQL = "CREATE TABLE mysql_source_tbl (\n" +
                "     id INT,\n" +
                "     name STRING,\n" +
                "     description STRING,\n" +
                "     PRIMARY KEY (id) NOT ENFORCED\n" +
                "   ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'tbds-172-16-16-142',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = 'portal@Tbds.com',\n" +
                "     'scan.startup.mode'='latest-offset',\n" +
                "     'database-name' = 'rison_db',\n" +
                "     'table-name' = 'student'\n" +
                "   )";
        TableResult tableResult = tableEnv.executeSql(mysqlCDCSQL);
        //创建kafka表
        String kafkaTableSQL = "CREATE TABLE kafka_tbl (\n" +
                "`id` INT,\n" +
                "`name` STRING,\n" +
                "`description` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'mysql_kafka_topic',\n" +
                "  'properties.bootstrap.servers' = 'tbds-172-16-16-144:6669,tbds-172-16-16-41:6669,tbds-172-16-16-91:6669',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'properties.security.protocol' = 'SASL_PLAINTEXT', \n" +
                "  'properties.sasl.mechanism' = 'PLAIN', \n" +
                "  'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka\" password=\"kafka@Tbds.com\";', \n" +
                "  'sink.partitioner' = 'round-robin',\n" +
                "  'value.format' = 'debezium-json', \n" +
                "  'value.debezium-json.ignore-parse-errors'='true' \n" +
                ")";
        tableEnv.executeSql(kafkaTableSQL);

        //插入数据
        tableEnv.executeSql("insert into kafka_tbl select * from mysql_source_tbl");
    }
}
