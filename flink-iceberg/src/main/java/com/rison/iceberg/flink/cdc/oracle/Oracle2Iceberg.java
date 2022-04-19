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
 * @PACKAGE_NAME: com.rison.iceberg.flink.read.cdc.debezium.mysql
 * @NAME: mysql2iceberg
 * @USER: Rison
 * @DATE: 2022/3/30 14:36
 * @PROJECT_NAME: iceberg
 **/
public class Oracle2Iceberg {
    private static String CHECKPOINT_DATA_URI = "hdfs:///flink/checkpoints-data/";
    private static String CATALOG_WAREHOUSE_LOCATION = "hdfs:///apps/hive/warehouse";
    private static String CATALOG_URI = "thrift://tbds-172-16-16-41:9083";

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
        Table sourceTable = tableEnv.sqlQuery("select * from oracle_source_tbl");

        //创建iceberg_catalog
        String catalogSQL = "CREATE CATALOG iceberg_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'uri'='thrift://tbds-172-16-16-41:9083',\n" +
                "  'clients'='5',\n" +
                "  'property-version'='1',\n" +
                "  'warehouse'='hdfs:///apps/hive/warehouse'\n" +
                ")";
        tableEnv.executeSql(catalogSQL);
        tableEnv.executeSql("use catalog iceberg_catalog");
        tableEnv.executeSql("use iceberg_db");

        String icebergCDCSQL = " CREATE TABLE if not exists iceberg_oracle_sink (\n" +
                " ID STRING,\n" +
                " NAME STRING,\n" +
                " DESCRIPTION STRING,\n" +
                " PRIMARY KEY (ID) NOT ENFORCED\n" +
                " ) WITH (\n" +
                " 'catalog-name'='iceberg_catalog',\n" +
                " 'catalog-type'='hive',  \n" +
                " 'uri'='thrift://tbds-172-16-16-41:9083',\n" +
                " 'warehouse'='hdfs:///apps/hive/warehouse',\n" +
                " 'format-version'='2',\n" +
                " 'database-name' = 'iceberg_db'\n" +
                " )";
        tableEnv.executeSql(icebergCDCSQL);

        // 将CDC数据源和下游数据表对接起来
        tableEnv.executeSql("INSERT INTO iceberg_oracle_sink SELECT * FROM " + sourceTable);

    }
}
