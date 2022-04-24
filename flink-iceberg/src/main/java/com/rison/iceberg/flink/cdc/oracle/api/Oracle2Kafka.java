package com.rison.iceberg.flink.cdc.oracle.api;

import com.rison.iceberg.flink.util.KafkaUtil;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;


/**
 * @PACKAGE_NAME: com.rison.iceberg.flink.cdc.oracle.api
 * @NAME: Oracle2Kafka
 * @USER: Rison
 * @DATE: 2022/4/20 0:56
 * @PROJECT_NAME: iceberg
 **/
public class Oracle2Kafka {
    private static String CHECKPOINT_DATA_URI = "hdfs:///flink/checkpoints-data/";
    private static String TOPIC = "oracle_cdc_kafka_topic_api";
    private static String KAFKA_SERVERS = "tbds-172-16-16-144:6669,tbds-172-16-16-41:6669,tbds-172-16-16-91:6669";
    public static void main(String[] args) throws Exception {
        //TODO 1. set flink env and set checkpoint
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        //TODO 2. source oracle-cdc
        Properties properties = new Properties();
//        properties.put("value.debezium-json.schema-include", "true");
//        properties.put("value.debezium-json.timestamp-format.standard", "SQL");

        SourceFunction<String> sourceFunction = OracleSource.<String>builder()
                .hostname("172.16.16.67")
                .port(1521)
                .database("XE")
                .schemaList("flinkuser")
                .tableList("flinkuser.oracle_source_tbl", "flinkuser.oracle_source_tbl_copy")
                .username("flinkuser")
                .password("flinkpw")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource
                .print().setParallelism(1);

        //TODO 3. sink kafka
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                TOPIC,
                new SimpleStringSchema(),
                KafkaUtil.producerProps(KAFKA_SERVERS)
        );
        dataStreamSource.addSink(kafkaProducer);

        env.execute("Oracle2Kafka api");

    }
}
/**
 {"before":null,"after":{"ID":"4007","NAME":"ZHANGSAN","DESCRIPTION":"ZHANG_DES"},"source":{"version":"1.5.4.Final","connector":"oracle","name":"oracle_logminer","ts_ms":1650598026000,"snapshot":"false","db":"XE","sequence":null,"schema":"FLINKUSER","table":"ORACLE_SOURCE_TBL","txId":"0a001900f1200000","scn":"3634640","commit_scn":"3638537","lcr_position":null},"op":"c","ts_ms":1650598060637,"transaction":null}
 {"before":null,"after":{"ID":"4007","NAME":"ZHANGSAN","DESCRIPTION":"ZHANG_DES"},"source":{"version":"1.5.4.Final","connector":"oracle","name":"oracle_logminer","ts_ms":1650598175000,"snapshot":"false","db":"XE","sequence":null,"schema":"FLINKUSER","table":"ORACLE_SOURCE_TBL_COPY","txId":"04001a009f060000","scn":"3681599","commit_scn":"3638537","lcr_position":null},"op":"c","ts_ms":1650598213271,"transaction":null}
 {"before":{"ID":"4007","NAME":"ZHANGSAN","DESCRIPTION":"ZHANG_DES"},"after":{"ID":"4007","NAME":"update-name","DESCRIPTION":"ZHANG_DES"},"source":{"version":"1.5.4.Final","connector":"oracle","name":"oracle_logminer","ts_ms":1650598175000,"snapshot":"false","db":"XE","sequence":null,"schema":"FLINKUSER","table":"ORACLE_SOURCE_TBL","txId":"04001a009f060000","scn":"3705954","commit_scn":"3638537","lcr_position":null},"op":"u","ts_ms":1650598213272,"transaction":null}
 {"before":{"ID":"4007","NAME":"ZHANGSAN","DESCRIPTION":"ZHANG_DES"},"after":{"ID":"4007","NAME":"update-name","DESCRIPTION":"ZHANG_DES"},"source":{"version":"1.5.4.Final","connector":"oracle","name":"oracle_logminer","ts_ms":1650598175000,"snapshot":"false","db":"XE","sequence":null,"schema":"FLINKUSER","table":"ORACLE_SOURCE_TBL_COPY","txId":"04001a009f060000","scn":"3711005","commit_scn":"3723778","lcr_position":null},"op":"u","ts_ms":1650598213272,"transaction":null}
 {"before":{"ID":"4007","NAME":"update-name","DESCRIPTION":"ZHANG_DES"},"after":null,"source":{"version":"1.5.4.Final","connector":"oracle","name":"oracle_logminer","ts_ms":1650598329000,"snapshot":"false","db":"XE","sequence":null,"schema":"FLINKUSER","table":"ORACLE_SOURCE_TBL_COPY","txId":"04000500cb060000","scn":"3809927","commit_scn":"3811904","lcr_position":null},"op":"d","ts_ms":1650598366611,"transaction":null}
 */
