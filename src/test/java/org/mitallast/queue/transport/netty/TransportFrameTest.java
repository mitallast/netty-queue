package org.mitallast.queue.transport.netty;

import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.stream.*;

import java.io.IOException;
import java.util.ArrayList;

public class TransportFrameTest extends BaseTest {

    @Test
    public void testMessage() throws Exception {
        StreamService streamService = new InternalStreamService(
            ImmutableSet.of(StreamableRegistry.of(TestStreamable.class, TestStreamable::new, 123)));
        StreamableEncoder encoder = new StreamableEncoder(streamService);
        StreamableDecoder decoder = new StreamableDecoder(streamService);

        ByteBuf buffer = Unpooled.buffer();
        encoder.encode(null, new TestStreamable(123123), buffer);

        ArrayList<Object> output = new ArrayList<>();
        decoder.decode(null, buffer, output);
        Assert.assertEquals(1, output.size());

        TestStreamable message = (TestStreamable) output.get(0);
        Assert.assertEquals(123123, message.value);
    }

    @Test
    public void testMessageEncodeBenchmark() throws Exception {
        StreamService streamService = new InternalStreamService(
            ImmutableSet.of(StreamableRegistry.of(TestStreamable.class, TestStreamable::new, 123)));
        StreamableEncoder encoder = new StreamableEncoder(streamService);

        ByteBuf buffer = Unpooled.directBuffer(1024);
        buffer.markWriterIndex();

        TestStreamable message = new TestStreamable(123123);

        int max = 100000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < max; i++) {
            buffer.resetWriterIndex();
            encoder.encode(null, message, buffer);
        }
        long end = System.currentTimeMillis();
        printQps("encode", max, start, end);
    }

    @Test
    public void testMessageDecodeBenchmark() throws Exception {
        StreamService streamService = new InternalStreamService(
            ImmutableSet.of(StreamableRegistry.of(TestStreamable.class, TestStreamable::new, 123)));
        StreamableEncoder encoder = new StreamableEncoder(streamService);
        StreamableDecoder decoder = new StreamableDecoder(streamService);

        ByteBuf buffer = Unpooled.directBuffer(1024);
        encoder.encode(null, new TestStreamable(123123), buffer);

        ArrayList<Object> output = new ArrayList<>(1);

        int max = 100000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < max; i++) {
            buffer.readerIndex(0);
            decoder.decode(null, buffer, output);
            output.clear();
        }
        long end = System.currentTimeMillis();
        printQps("decode", max, start, end);
    }

    public static class TestStreamable implements Streamable {

        private final long value;

        public TestStreamable(StreamInput streamInput) throws IOException {
            this.value = streamInput.readLong();
        }

        public TestStreamable(long value) {
            this.value = value;
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(value);
        }
    }
}
