//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.streaming.connectors.kafka;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/** @deprecated */
@Deprecated
@Internal
public abstract class KafkaTableSinkBase implements AppendStreamTableSink<Row> {
    private final TableSchema schema;
    protected final String topic;
    protected final Properties properties;
    protected final SerializationSchema<Row> serializationSchema;
    protected final Optional<FlinkKafkaPartitioner<Row>> partitioner;

    protected KafkaTableSinkBase(TableSchema schema, String topic, Properties properties, Optional<FlinkKafkaPartitioner<Row>> partitioner, SerializationSchema<Row> serializationSchema) {
        this.schema = TableSchemaUtils.checkOnlyPhysicalColumns(schema);
        this.topic = (String)Preconditions.checkNotNull(topic, "Topic must not be null.");
        this.properties = (Properties)Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.partitioner = (Optional)Preconditions.checkNotNull(partitioner, "Partitioner must not be null.");
        this.serializationSchema = (SerializationSchema)Preconditions.checkNotNull(serializationSchema, "Serialization schema must not be null.");
    }

    protected abstract SinkFunction<Row> createKafkaProducer(String var1, Properties var2, SerializationSchema<Row> var3, Optional<FlinkKafkaPartitioner<Row>> var4);

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        SinkFunction<Row> kafkaProducer = this.createKafkaProducer(this.topic, this.properties, this.serializationSchema, this.partitioner);
        return dataStream.addSink(kafkaProducer).setParallelism(dataStream.getParallelism()).name(TableConnectorUtils.generateRuntimeName(this.getClass(), this.getFieldNames()));
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return this.schema.toRowType();
    }

    @Override
    public String[] getFieldNames() {
        return this.schema.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return this.schema.getFieldTypes();
    }

    @Override
    public KafkaTableSinkBase configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (Arrays.equals(this.getFieldNames(), fieldNames) && Arrays.equals(this.getFieldTypes(), fieldTypes)) {
            return this;
        } else {
            throw new ValidationException("Reconfiguration with different fields is not allowed. Expected: " + Arrays.toString(this.getFieldNames()) + " / " + Arrays.toString(this.getFieldTypes()) + ". But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            KafkaTableSinkBase that = (KafkaTableSinkBase)o;
            return Objects.equals(this.schema, that.schema) && Objects.equals(this.topic, that.topic) && Objects.equals(this.properties, that.properties) && Objects.equals(this.serializationSchema, that.serializationSchema) && Objects.equals(this.partitioner, that.partitioner);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(new Object[]{this.schema, this.topic, this.properties, this.serializationSchema, this.partitioner});
    }
}
