package com.rison.kafka.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rison.kafka.util.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.TableLoader;
import org.apache.flink.table.data.RowData;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @PACKAGE_NAME: com.rison.kafka.app
 * @NAME: Kafka2Iceberg
 * @USER: Rison
 * @DATE: 2022/2/9 9:23
 * @PROJECT_NAME: flink-iceberg
 **/

public class Kafka2IcebergDemo {
    private final static String ICEBERG_DEFAULT_DB = "iceberg_db";
    private static String ICEBERG_DEFAULT_TABLE = "iceberg_default_table";
    private static String CATALOG_WAREHOUSE_LOCATION = "hdfs:///apps/hive/warehouse";
    private static String CATALOG_URI = "thrift://tbds-172-16-16-41:9083";
    private static String CATALOG = "iceberg_catalog";
    private static String TOPIC = "kafka_iceberg_topic";
    private static String SERVERS = "tbds-172-16-16-144:6669,tbds-172-16-16-41:6669,tbds-172-16-16-91:6669";

    public static void main(String[] args) throws Exception {
        //TODO 1. set flink env and set checkpoint
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //为了验证，这里设置了1分钟
        checkpointConfig.setCheckpointInterval(1_000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(1_1000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        checkpointConfig.setCheckpointTimeout(60_1000L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoints-data/"));
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.setParallelism(1);

        //TODO 2. consume kafka data
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC,
                new SimpleStringSchema(),
                KafkaUtil.consumerProps(SERVERS, "icebergGroup")
        );
        SingleOutputStreamOperator<RowData> kafkaStream = env.addSource(kafkaConsumer).map(
                new MapFunction<String, RowData>() {
                    @Override
                    public RowData map(String value) throws Exception {
                        JSONObject dataJson = JSON.parseObject(value);
                        GenericRowData row = new GenericRowData(5);
                        row.setField(0, StringData.fromBytes(dataJson.getString("id").getBytes()));
                        row.setField(1, StringData.fromBytes(dataJson.getString("name").getBytes()));
                        row.setField(2, dataJson.getInteger("age"));
                        row.setField(3, StringData.fromBytes(dataJson.getString("sex").getBytes()));
                        row.setField(4, TimestampData.fromEpochMillis(dataJson.getLong("ts")));
                        return row;
                    }
                }
        );
        kafkaStream.print();
        // uid is used for job restart or something when using savepoint.
        kafkaStream.uid("icebergGroup");

        //TODO 3. create iceberg table
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(ICEBERG_DEFAULT_DB), "iceberg_table_kafka");
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.IntegerType.get()),
                Types.NestedField.required(4, "sex", Types.StringType.get()),
                Types.NestedField.required(5, "ts", Types.TimestampType.withoutZone()));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name());
        CatalogLoader catalogLoader = getCatalogLoader(CATALOG);
        Catalog catalog = catalogLoader.loadCatalog();
        Table table = null;
        if (!catalog.tableExists(identifier)) {
            table = catalog.createTable(identifier, schema, spec, props);
        } else {
            table = catalog.loadTable(identifier);
        }
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

        //TODO 4. sink kafka data to iceberg
        FlinkSink.forRowData(kafkaStream)
                .table(table)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .build();

        //TODO 5. execute
        env.execute("kafka2iceberg");
    }


    /**
     * 获取catalogLoader
     *
     * @param catalog
     * @return
     */
    private static CatalogLoader getCatalogLoader(String catalog) throws IOException {
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "iceberg");
        catalogProperties.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HIVE);
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, CATALOG_WAREHOUSE_LOCATION);
        catalogProperties.put(CatalogProperties.URI, CATALOG_URI);
        catalogProperties.put(CatalogProperties.CLIENT_POOL_SIZE, "5");
        CatalogLoader catalogLoader = CatalogLoader.hive(catalog, getHadoopConfig(), catalogProperties);
        return catalogLoader;

    }

    /**
     * 获取hadoop config
     *
     * @return
     * @throws IOException
     */
    public static Configuration getHadoopConfig() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hdfsCluster");
        conf.addResource(new Path("/usr/hdp/current/hadoop-client/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/hdp/current/hadoop-client/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/hdp/current/hive-client/conf/hive-site.xml"));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(null);
        return conf;
    }
}
