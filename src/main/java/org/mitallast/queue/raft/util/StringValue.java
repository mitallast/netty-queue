package org.mitallast.queue.raft.util;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class StringValue implements Streamable {

    private String value;

    public StringValue() {
    }

    public StringValue(String value) {
        this.value = value;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        value = stream.readText();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeText(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StringValue that = (StringValue) o;

        return !(value != null ? !value.equals(that.value) : that.value != null);

    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "StringValue{" +
            "value='" + value + '\'' +
            '}';
    }
}
