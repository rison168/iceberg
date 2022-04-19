//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.streaming.connectors.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

/** @deprecated */
@Deprecated
public abstract class KafkaTableSourceSinkFactoryBase implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Row> {
    public KafkaTableSourceSinkFactoryBase() {
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap();
        context.put("connector.type", "kafka");
        context.put("connector.version", this.kafkaVersion());
        context.put("connector.property-version", "1");
        return context;
    }

    public List<String> supportedProperties() {
        List<String> properties = new ArrayList();
        properties.add("update-mode");
        properties.add("connector.topic");
        properties.add("connector.properties");
        properties.add("connector.properties.#.key");
        properties.add("connector.properties.#.value");
        properties.add("connector.properties.*");
        properties.add("connector.startup-mode");
        properties.add("connector.specific-offsets");
        properties.add("connector.specific-offsets.#.partition");
        properties.add("connector.specific-offsets.#.offset");
        properties.add("connector.startup-timestamp-millis");
        properties.add("connector.sink-partitioner");
        properties.add("connector.sink-partitioner-class");
        properties.add("schema.#.data-type");
        properties.add("schema.#.type");
        properties.add("schema.#.name");
        properties.add("schema.#.from");
        properties.add("schema.#.expr");
        properties.add("schema.#.proctime");
        properties.add("schema.#.rowtime.timestamps.type");
        properties.add("schema.#.rowtime.timestamps.from");
        properties.add("schema.#.rowtime.timestamps.class");
        properties.add("schema.#.rowtime.timestamps.serialized");
        properties.add("schema.#.rowtime.watermarks.type");
        properties.add("schema.#.rowtime.watermarks.class");
        properties.add("schema.#.rowtime.watermarks.serialized");
        properties.add("schema.#.rowtime.watermarks.delay");
        properties.add("schema.watermark.#.rowtime");
        properties.add("schema.watermark.#.strategy.expr");
        properties.add("schema.watermark.#.strategy.data-type");
        properties.add("schema.primary-key.name");
        properties.add("schema.primary-key.columns");
        properties.add("comment");
        properties.add("format.*");
        return properties;
    }

    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = this.getValidatedProperties(properties);
        String topic = descriptorProperties.getString("connector.topic");
        DeserializationSchema<Row> deserializationSchema = this.getDeserializationSchema(properties);
        KafkaTableSourceSinkFactoryBase.StartupOptions startupOptions = this.getStartupOptions(descriptorProperties, topic);
        return this.createKafkaTableSource(TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema("schema")), SchemaValidator.deriveProctimeAttribute(descriptorProperties), SchemaValidator.deriveRowtimeAttributes(descriptorProperties), SchemaValidator.deriveFieldMapping(descriptorProperties, Optional.of(deserializationSchema.getProducedType())), topic, this.getKafkaProperties(descriptorProperties), deserializationSchema, startupOptions.startupMode, startupOptions.specificOffsets, startupOptions.startupTimestampMillis);
    }

    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = this.getValidatedProperties(properties);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema("schema"));
        String topic = descriptorProperties.getString("connector.topic");
        Optional<String> proctime = SchemaValidator.deriveProctimeAttribute(descriptorProperties);
        List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors = SchemaValidator.deriveRowtimeAttributes(descriptorProperties);
        if (!proctime.isPresent() && rowtimeAttributeDescriptors.isEmpty() && !this.checkForCustomFieldMapping(descriptorProperties, schema)) {
            return this.createKafkaTableSink(schema, topic, this.getKafkaProperties(descriptorProperties), this.getFlinkKafkaPartitioner(descriptorProperties), this.getSerializationSchema(properties));
        } else {
            throw new TableException("Time attributes and custom field mappings are not supported yet.");
        }
    }

    protected abstract String kafkaVersion();

    protected abstract boolean supportsKafkaTimestamps();

    protected abstract KafkaTableSourceBase createKafkaTableSource(TableSchema var1, Optional<String> var2, List<RowtimeAttributeDescriptor> var3, Map<String, String> var4, String var5, Properties var6, DeserializationSchema<Row> var7, StartupMode var8, Map<KafkaTopicPartition, Long> var9, long var10);

    protected abstract KafkaTableSinkBase createKafkaTableSink(TableSchema var1, String var2, Properties var3, Optional<FlinkKafkaPartitioner<Row>> var4, SerializationSchema<Row> var5);

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        (new SchemaValidator(true, this.supportsKafkaTimestamps(), false)).validate(descriptorProperties);
        (new KafkaValidator()).validate(descriptorProperties);
        return descriptorProperties;
    }

    private DeserializationSchema<Row> getDeserializationSchema(Map<String, String> properties) {
        DeserializationSchemaFactory<Row> formatFactory = (DeserializationSchemaFactory)TableFactoryService.find(DeserializationSchemaFactory.class, properties, this.getClass().getClassLoader());
        return formatFactory.createDeserializationSchema(properties);
    }

    private SerializationSchema<Row> getSerializationSchema(Map<String, String> properties) {
        SerializationSchemaFactory<Row> formatFactory = (SerializationSchemaFactory)TableFactoryService.find(SerializationSchemaFactory.class, properties, this.getClass().getClassLoader());
        return formatFactory.createSerializationSchema(properties);
    }

    private Properties getKafkaProperties(DescriptorProperties descriptorProperties) {
        Properties kafkaProperties = new Properties();
        if (KafkaValidator.hasConciseKafkaProperties(descriptorProperties)) {
            descriptorProperties.asMap().keySet().stream().filter((key) -> {
                return key.startsWith("connector.properties");
            }).forEach((key) -> {
                String value = descriptorProperties.getString(key);
                String subKey = key.substring("connector.properties.".length());
                kafkaProperties.put(subKey, value);
            });
        } else {
            List<Map<String, String>> propsList = descriptorProperties.getFixedIndexedProperties("connector.properties", Arrays.asList("key", "value"));
            propsList.forEach((kv) -> {
                kafkaProperties.put(descriptorProperties.getString((String)kv.get("key")), descriptorProperties.getString((String)kv.get("value")));
            });
        }

        return kafkaProperties;
    }

    private KafkaTableSourceSinkFactoryBase.StartupOptions getStartupOptions(DescriptorProperties descriptorProperties, String topic) {
        Map<KafkaTopicPartition, Long> specificOffsets = new HashMap();
        StartupMode startupMode = (StartupMode)descriptorProperties.getOptionalString("connector.startup-mode").map((modeString) -> {
            byte var6 = -1;
            switch(modeString.hashCode()) {
                case -1390285235:
                    if (modeString.equals("earliest-offset")) {
                        var6 = 0;
                    }
                    break;
                case -410146651:
                    if (modeString.equals("specific-offsets")) {
                        var6 = 3;
                    }
                    break;
                case 55126294:
                    if (modeString.equals("timestamp")) {
                        var6 = 4;
                    }
                    break;
                case 514263449:
                    if (modeString.equals("latest-offset")) {
                        var6 = 1;
                    }
                    break;
                case 1556617458:
                    if (modeString.equals("group-offsets")) {
                        var6 = 2;
                    }
            }

            switch(var6) {
                case 0:
                    return StartupMode.EARLIEST;
                case 1:
                    return StartupMode.LATEST;
                case 2:
                    return StartupMode.GROUP_OFFSETS;
                case 3:
                    this.buildSpecificOffsets(descriptorProperties, topic, specificOffsets);
                    return StartupMode.SPECIFIC_OFFSETS;
                case 4:
                    return StartupMode.TIMESTAMP;
                default:
                    throw new TableException("Unsupported startup mode. Validator should have checked that.");
            }
        }).orElse(StartupMode.GROUP_OFFSETS);
        KafkaTableSourceSinkFactoryBase.StartupOptions options = new KafkaTableSourceSinkFactoryBase.StartupOptions();
        options.startupMode = startupMode;
        options.specificOffsets = specificOffsets;
        if (startupMode == StartupMode.TIMESTAMP) {
            options.startupTimestampMillis = descriptorProperties.getLong("connector.startup-timestamp-millis");
        }

        return options;
    }

    private void buildSpecificOffsets(DescriptorProperties descriptorProperties, String topic, Map<KafkaTopicPartition, Long> specificOffsets) {
        if (descriptorProperties.containsKey("connector.specific-offsets")) {
            Map<Integer, Long> offsetMap = KafkaValidator.validateAndParseSpecificOffsetsString(descriptorProperties);
            offsetMap.forEach((partition, offset) -> {
                KafkaTopicPartition topicPartition = new KafkaTopicPartition(topic, partition);
                specificOffsets.put(topicPartition, offset);
            });
        } else {
            List<Map<String, String>> offsetList = descriptorProperties.getFixedIndexedProperties("connector.specific-offsets", Arrays.asList("partition", "offset"));
            offsetList.forEach((kv) -> {
                int partition = descriptorProperties.getInt((String)kv.get("partition"));
                long offset = descriptorProperties.getLong((String)kv.get("offset"));
                KafkaTopicPartition topicPartition = new KafkaTopicPartition(topic, partition);
                specificOffsets.put(topicPartition, offset);
            });
        }

    }

    private Optional<FlinkKafkaPartitioner<Row>> getFlinkKafkaPartitioner(DescriptorProperties descriptorProperties) {
        return descriptorProperties.getOptionalString("connector.sink-partitioner").flatMap((partitionerString) -> {
            byte var3 = -1;
            switch(partitionerString.hashCode()) {
                case -1662301013:
                    if (partitionerString.equals("round-robin")) {
                        var3 = 1;
                    }
                    break;
                case -1349088399:
                    if (partitionerString.equals("custom")) {
                        var3 = 2;
                    }
                    break;
                case 97445748:
                    if (partitionerString.equals("fixed")) {
                        var3 = 0;
                    }
            }

            switch(var3) {
                case 0:
                    return Optional.of(new FlinkFixedPartitioner());
                case 1:
                    return Optional.empty();
                case 2:
                    Class<? extends FlinkKafkaPartitioner> partitionerClass = descriptorProperties.getClass("connector.sink-partitioner-class", FlinkKafkaPartitioner.class);
                    return Optional.of((FlinkKafkaPartitioner)InstantiationUtil.instantiate(partitionerClass));
                default:
                    throw new TableException("Unsupported sink partitioner. Validator should have checked that.");
            }
        });
    }

    private boolean checkForCustomFieldMapping(DescriptorProperties descriptorProperties, TableSchema schema) {
        Map<String, String> fieldMapping = SchemaValidator.deriveFieldMapping(descriptorProperties, Optional.of(schema.toRowType()));
        return fieldMapping.size() != schema.getFieldNames().length || !fieldMapping.entrySet().stream().allMatch((mapping) -> {
            return ((String)mapping.getKey()).equals(mapping.getValue());
        });
    }

    private static class StartupOptions {
        private StartupMode startupMode;
        private Map<KafkaTopicPartition, Long> specificOffsets;
        private long startupTimestampMillis;

        private StartupOptions() {
        }
    }
}
