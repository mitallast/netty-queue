package org.mitallast.queue.raft.resource.result;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class StringListResult implements Streamable {
    private ImmutableList<String> value;

    public StringListResult() {
        value = ImmutableList.of();
    }

    public StringListResult(ImmutableList<String> value) {
        this.value = value;
    }

    public ImmutableList<String> get() {
        return value;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        int size = stream.readInt();
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (int i = 0; i < size; i++) {
            builder.add(stream.readText());
        }
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeInt(value.size());
        for (String string : value) {
            stream.writeText(string);
        }
    }
}
