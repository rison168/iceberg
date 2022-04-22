package com.rison.iceberg.flink.cdc.mysql.api;

import com.rison.iceberg.flink.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;


/**
 * @PACKAGE_NAME: com.rison.iceberg.flink.cdc.mysql.api
 * @NAME: Mysql2Kafka
 * @USER: Rison
 * @DATE: 2022/4/20 0:56
 * @PROJECT_NAME: iceberg
 **/
public class Mysql2Kafka {
    private static String CHECKPOINT_DATA_URI = "hdfs:///flink/checkpoints-data/";
    private static String TOPIC = "mysql_cdc_kafka_topic_api";
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
        //TODO 2. source mysql-cdc
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("tbds-172-16-16-142")
                .port(3306)
                .username("root")
                .password("portal@Tbds.com")
                .databaseList("rison_db")
                .tableList("rison_db.student", "rison_db.student_copy")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        dataStreamSource.setParallelism(4)
                .print().setParallelism(1);

        //TODO 3. sink kafka
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                TOPIC,
                new SimpleStringSchema(),
                KafkaUtil.producerProps(KAFKA_SERVERS)
        );
        dataStreamSource.addSink(kafkaProducer);

        env.execute("Mysql2Kafka api");

    }
}

/**
 {"before":null,"after":{"id":1,"name":"rison","description":"description_01"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"rison_db","sequence":null,"table":"student","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1650596456275,"transaction":null}
 {"before":null,"after":{"id":21,"name":"数据1","description":"description_数据1"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1650596656000,"snapshot":"false","db":"rison_db","sequence":null,"table":"student","server_id":1,"gtid":null,"file":"binlog.000011","pos":1019565335,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1650596656240,"transaction":null}
 {"before":{"id":19,"name":"数据1","description":"description_数据1"},"after":{"id":19,"name":"update_数据","description":"description_数据1"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1650596703000,"snapshot":"false","db":"rison_db","sequence":null,"table":"student","server_id":1,"gtid":null,"file":"binlog.000011","pos":1019607925,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1650596703610,"transaction":null}
 {"before":{"id":19,"name":"update_数据","description":"description_数据1"},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1650596730000,"snapshot":"false","db":"rison_db","sequence":null,"table":"student","server_id":1,"gtid":null,"file":"binlog.000011","pos":1019622862,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1650596730332,"transaction":null}

 */
