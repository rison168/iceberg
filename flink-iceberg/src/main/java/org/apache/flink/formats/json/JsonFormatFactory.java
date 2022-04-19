//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.formats.json;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions.MapNullKeyMode;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

public class JsonFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {
    public static final String IDENTIFIER = "json";

    public JsonFormatFactory() {
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        JsonOptions.validateDecodingFormatOptions(formatOptions);
        final boolean failOnMissingField = (Boolean)formatOptions.get(JsonOptions.FAIL_ON_MISSING_FIELD);
        final boolean ignoreParseErrors = (Boolean)formatOptions.get(JsonOptions.IGNORE_PARSE_ERRORS);
        final TimestampFormat timestampOption = JsonOptions.getTimestampFormat(formatOptions);
        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(org.apache.flink.table.connector.source.DynamicTableSource.Context context, DataType producedDataType) {
                RowType rowType = (RowType)producedDataType.getLogicalType();
                TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(producedDataType);
                return new JsonRowDataDeserializationSchema(rowType, rowDataTypeInfo, failOnMissingField, ignoreParseErrors, timestampOption);
            }

            @Override
            public ChangelogMode getChangelogMode() {

//                return ChangelogMode.insertOnly();
                return ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_BEFORE)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        JsonOptions.validateEncodingFormatOptions(formatOptions);
        final TimestampFormat timestampOption = JsonOptions.getTimestampFormat(formatOptions);
        final MapNullKeyMode mapNullKeyMode = JsonOptions.getMapNullKeyMode(formatOptions);
        final String mapNullKeyLiteral = (String)formatOptions.get(JsonOptions.MAP_NULL_KEY_LITERAL);
        final boolean encodeDecimalAsPlainNumber = (Boolean)formatOptions.get(JsonOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(org.apache.flink.table.connector.sink.DynamicTableSink.Context context, DataType consumedDataType) {
                RowType rowType = (RowType)consumedDataType.getLogicalType();
                return new JsonRowDataSerializationSchema(rowType, timestampOption, mapNullKeyMode, mapNullKeyLiteral, encodeDecimalAsPlainNumber);
            }

            @Override
            public ChangelogMode getChangelogMode() {

//                return ChangelogMode.insertOnly();
                return ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_BEFORE)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return "json";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(JsonOptions.FAIL_ON_MISSING_FIELD);
        options.add(JsonOptions.IGNORE_PARSE_ERRORS);
        options.add(JsonOptions.TIMESTAMP_FORMAT);
        options.add(JsonOptions.MAP_NULL_KEY_MODE);
        options.add(JsonOptions.MAP_NULL_KEY_LITERAL);
        options.add(JsonOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        return options;
    }
}
