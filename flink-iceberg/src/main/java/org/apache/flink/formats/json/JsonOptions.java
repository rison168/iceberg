//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.formats.json;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

public class JsonOptions {
    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = ConfigOptions.key("fail-on-missing-field").booleanType().defaultValue(false).withDescription("Optional flag to specify whether to fail if a field is missing or not, false by default.");
    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions.key("ignore-parse-errors").booleanType().defaultValue(false).withDescription("Optional flag to skip fields and rows with parse errors instead of failing;\nfields are set to null in case of errors, false by default.");
    public static final ConfigOption<String> MAP_NULL_KEY_MODE = ConfigOptions.key("map-null-key.mode").stringType().defaultValue("FAIL").withDescription("Optional flag to control the handling mode when serializing null key for map data, FAIL by default. Option DROP will drop null key entries for map data. Option LITERAL will use 'map-null-key.literal' as key literal.");
    public static final ConfigOption<String> MAP_NULL_KEY_LITERAL = ConfigOptions.key("map-null-key.literal").stringType().defaultValue("null").withDescription("Optional flag to specify string literal for null keys when 'map-null-key.mode' is LITERAL, \"null\" by default.");
    public static final ConfigOption<String> TIMESTAMP_FORMAT = ConfigOptions.key("timestamp-format.standard").stringType().defaultValue("SQL").withDescription("Optional flag to specify timestamp format, SQL by default. Option ISO-8601 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}\" format and output timestamp in the same format. Option SQL will parse input timestamp in \"yyyy-MM-dd HH:mm:ss.s{precision}\" format and output timestamp in the same format.");
    public static final ConfigOption<Boolean> ENCODE_DECIMAL_AS_PLAIN_NUMBER = ConfigOptions.key("encode.decimal-as-plain-number").booleanType().defaultValue(false).withDescription("Optional flag to specify whether to encode all decimals as plain numbers instead of possible scientific notations, false by default.");
    public static final String SQL = "SQL";
    public static final String ISO_8601 = "ISO-8601";
    public static final Set<String> TIMESTAMP_FORMAT_ENUM = new HashSet(Arrays.asList("SQL", "ISO-8601"));
    public static final String JSON_MAP_NULL_KEY_MODE_FAIL = "FAIL";
    public static final String JSON_MAP_NULL_KEY_MODE_DROP = "DROP";
    public static final String JSON_MAP_NULL_KEY_MODE_LITERAL = "LITERAL";

    public JsonOptions() {
    }

    public static TimestampFormat getTimestampFormat(ReadableConfig config) {
        String timestampFormat = (String)config.get(TIMESTAMP_FORMAT);
        byte var3 = -1;
        switch(timestampFormat.hashCode()) {
            case 82350:
                if (timestampFormat.equals("SQL")) {
                    var3 = 0;
                }
                break;
            case 1329480167:
                if (timestampFormat.equals("ISO-8601")) {
                    var3 = 1;
                }
        }

        switch(var3) {
            case 0:
                return TimestampFormat.SQL;
            case 1:
                return TimestampFormat.ISO_8601;
            default:
                throw new TableException(String.format("Unsupported timestamp format '%s'. Validator should have checked that.", timestampFormat));
        }
    }

    public static JsonOptions.MapNullKeyMode getMapNullKeyMode(ReadableConfig config) {
        String mapNullKeyMode = (String)config.get(MAP_NULL_KEY_MODE);
        String var2 = mapNullKeyMode.toUpperCase();
        byte var3 = -1;
        switch(var2.hashCode()) {
            case 2107119:
                if (var2.equals("DROP")) {
                    var3 = 1;
                }
                break;
            case 2150174:
                if (var2.equals("FAIL")) {
                    var3 = 0;
                }
                break;
            case 900443279:
                if (var2.equals("LITERAL")) {
                    var3 = 2;
                }
        }

        switch(var3) {
            case 0:
                return JsonOptions.MapNullKeyMode.FAIL;
            case 1:
                return JsonOptions.MapNullKeyMode.DROP;
            case 2:
                return JsonOptions.MapNullKeyMode.LITERAL;
            default:
                throw new TableException(String.format("Unsupported map null key handling mode '%s'. Validator should have checked that.", mapNullKeyMode));
        }
    }

    public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
        boolean failOnMissingField = (Boolean)tableOptions.get(FAIL_ON_MISSING_FIELD);
        boolean ignoreParseErrors = (Boolean)tableOptions.get(IGNORE_PARSE_ERRORS);
        if (ignoreParseErrors && failOnMissingField) {
            throw new ValidationException(FAIL_ON_MISSING_FIELD.key() + " and " + IGNORE_PARSE_ERRORS.key() + " shouldn't both be true.");
        } else {
            validateTimestampFormat(tableOptions);
        }
    }

    public static void validateEncodingFormatOptions(ReadableConfig tableOptions) {
        Set<String> nullKeyModes = (Set)Arrays.stream(JsonOptions.MapNullKeyMode.values()).map(Objects::toString).collect(Collectors.toSet());
        if (!nullKeyModes.contains(((String)tableOptions.get(MAP_NULL_KEY_MODE)).toUpperCase())) {
            throw new ValidationException(String.format("Unsupported value '%s' for option %s. Supported values are %s.", tableOptions.get(MAP_NULL_KEY_MODE), MAP_NULL_KEY_MODE.key(), nullKeyModes));
        } else {
            validateTimestampFormat(tableOptions);
        }
    }

    static void validateTimestampFormat(ReadableConfig tableOptions) {
        String timestampFormat = (String)tableOptions.get(TIMESTAMP_FORMAT);
        if (!TIMESTAMP_FORMAT_ENUM.contains(timestampFormat)) {
            throw new ValidationException(String.format("Unsupported value '%s' for %s. Supported values are [SQL, ISO-8601].", timestampFormat, TIMESTAMP_FORMAT.key()));
        }
    }

    public static enum MapNullKeyMode {
        FAIL,
        DROP,
        LITERAL;

        private MapNullKeyMode() {
        }
    }
}
