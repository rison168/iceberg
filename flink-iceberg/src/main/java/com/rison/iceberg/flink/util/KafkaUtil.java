package com.rison.iceberg.flink.util;

import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.StringSerializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

/**
 * @PACKAGE_NAME: com.rison.iceberg.flink.util
 * @NAME: KafkaUtil
 * @USER: Rison
 * @DATE: 2022/3/24 10:37
 * @PROJECT_NAME: iceberg
 **/
public class KafkaUtil {
    /**
     * kafka 消费者 properties
     * @param servers kafka broker servers
     * @param groupId groupId
     * @return
     */
    public static Properties consumerProps(String servers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "latest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "3000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        //设置SASL_PLAINT认证
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka\" password=\"kafka@Tbds.com\";");
        return props;
    }

    /**
     * kafka 生产者 properties
     * @param servers kafka broker servers
     * @return
     */
    public static Properties producerProps(String servers){
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("acks", "all");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        //设置SASL_PLAINT认证
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka\" password=\"kafka@Tbds.com\";");
        return props;
    }
}

