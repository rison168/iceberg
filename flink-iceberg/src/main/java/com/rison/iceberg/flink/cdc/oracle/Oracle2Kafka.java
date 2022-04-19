package com.rison.iceberg.flink.cdc.oracle;

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
 * @PACKAGE_NAME: com.rison.iceberg.flink.cdc.oracle
 * @NAME: oracle2kafka
 * @USER: Rison
 * @DATE: 2022/4/20 0:55
 * @PROJECT_NAME: iceberg
 **/
public class Oracle2Kafka {
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
        // 创建表，connector使用oracle-cdc
        String oracleCDCSQL = "CREATE TABLE oracle_source_tbl(\n" +
                "ID STRING,\n" +
                "NAME STRING,\n" +
                "DESCRIPTION STRING,\n" +
                "PRIMARY KEY(ID) NOT ENFORCED \n" +
                ") WITH (\n" +
                "'connector' = 'oracle-cdc',\n" +
                "'hostname' = '172.16.16.67',\n" +
                "'port' = '1521',\n" +
                "'username' = 'flinkuser',\n" +
                "'password' = 'flinkpw',\n" +
                "'database-name' = 'xe',\n" +
                "'schema-name' = 'flinkuser',\n" +
                "'debezium.log.mining.continuous.mine' = 'true',\n" +
                "'debezium.log.mining.strategy' = 'online_catalog',\n" +
                "'table-name' = 'oracle_source_tbl',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'debezium.database.tablename.case.insensitive'='false'\n" +
                ")";
        TableResult tableResult = tableEnv.executeSql(oracleCDCSQL);
        //创建kafka表
        String kafkaTableSQL = "CREATE TABLE kafka_tbl (\n" +
                "`ID` STRING,\n" +
                "`NAME` STRING,\n" +
                "`DESCRIPTION` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'oracle_kafka_topic',\n" +
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
        tableEnv.executeSql("insert into kafka_tbl select * from oracle_source_tbl");


    }
}
