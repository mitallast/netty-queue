package org.mitallast.queue.common.unit;

import org.mitallast.queue.QueueParseException;

import static org.mitallast.queue.common.unit.ByteSizeValue.parseBytesSizeValue;

public class MemorySizeValue {
    public static ByteSizeValue parseBytesSizeValueOrHeapRatio(String sValue) {
        if (sValue.endsWith("%")) {
            final String percentAsString = sValue.substring(0, sValue.length() - 1);
            try {
                final double percent = Double.parseDouble(percentAsString);
                if (percent < 0 || percent > 100) {
                    throw new QueueParseException("Percentage should be in [0-100], got " + percentAsString);
                }
                return new ByteSizeValue((long) ((percent / 100) * Runtime.getRuntime().maxMemory()), ByteSizeUnit.BYTES);
            } catch (NumberFormatException e) {
                throw new QueueParseException("Failed to parse [" + percentAsString + "] as a double", e);
            }
        } else {
            return parseBytesSizeValue(sValue);
        }
    }
}
