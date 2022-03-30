package com.rison.iceberg.flink.read.kafka;

import com.alibaba.fastjson.JSONObject;
import com.rison.iceberg.flink.util.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @PACKAGE_NAME: com.rison.kafka.app
 * @NAME: Icebeg2Kafka
 * @USER: Rison
 * @DATE: 2022/2/9 16:09
 * @PROJECT_NAME: flink-iceberg-demo
 **/
public class Iceberg2Kafka {
    private final static String ICEBERG_DEFAULT_DB = "iceberg_db";
    private static String ICEBERG_DEFAULT_TABLE = "iceberg_table_kafka";
    private static String CATALOG_WAREHOUSE_LOCATION = "hdfs:///apps/hive/warehouse";
    private static String CATALOG_URI = "thrift://tbds-172-16-16-41:9083";
    private static String CATALOG = "iceberg_catalog";
    private static String TOPIC = "iceberg_kafka_topic";
    private static String KAFKA_SERVERS = "tbds-172-16-16-144:6669,tbds-172-16-16-41:6669,tbds-172-16-16-91:6669";
    private static String CHECKPOINT_DATA_URI = "hdfs:///flink/checkpoints-data/";

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
        env.setStateBackend(new FsStateBackend(CHECKPOINT_DATA_URI));
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.setParallelism(1);

        //TODO 2. select iceberg table
        TableIdentifier identifier = TableIdentifier.of(Namespace.of(ICEBERG_DEFAULT_DB), ICEBERG_DEFAULT_TABLE);
        CatalogLoader catalogLoader = getCatalogLoader(CATALOG);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);
        DataStream<RowData> batchStream = FlinkSource
                .forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                .build();
        batchStream.print();
        SingleOutputStreamOperator<String> dataStream = batchStream.map(new MapFunction<RowData, String>() {
            private static final long serialVersionUID = 3149495323490165536L;

            @Override
            public String map(RowData value) throws Exception {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("id", value.getString(0).toString());
                jsonObject.put("name", value.getString(1).toString());
                jsonObject.put("age", value.getInt(2));
                jsonObject.put("sex", value.getString(3).toString());
                jsonObject.put("ts", value.getTimestamp(4, 6).getMillisecond());
                System.out.println(jsonObject.toJSONString());
                return jsonObject.toJSONString();
            }
        });

        //TODO 3. sink kafka
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                TOPIC,
                new SimpleStringSchema(),
                KafkaUtil.producerProps(KAFKA_SERVERS)
        );
        dataStream.addSink(kafkaProducer);

        //TODO 4. execute
        env.execute("iceberg2kafka");


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
