package org.apache.flink.formats.common;

import org.apache.flink.annotation.Internal;

@Internal
public enum TimestampFormat {
    SQL,
    ISO_8601;

    private TimestampFormat() {
    }
}
