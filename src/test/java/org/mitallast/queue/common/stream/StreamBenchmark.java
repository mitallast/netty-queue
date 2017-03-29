package org.mitallast.queue.common.stream;

import com.google.common.collect.ImmutableSet;
import com.typesafe.config.ConfigFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

import java.io.IOException;

public class StreamBenchmark extends BaseTest {
    private ByteBuf buffer;
    private StreamInput input;
    private StreamOutput output;

    @Override
    protected int max() {
        return 100000;
    }

    @Before
    public void setUp() throws Exception {
        StreamService streamService = new InternalStreamService(ImmutableSet.of(StreamableRegistry.of(TestStreamable.class, TestStreamable::new, 123)));
        buffer = Unpooled.buffer();
        output = streamService.output(buffer);
        output.writeClass(TestStreamable.class);
        input = streamService.input(buffer);
    }

    @After
    public void tearDown() throws Exception {
        input.close();
        output.close();
        buffer.release();
    }

    @Test
    public void testWriteInt() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            buffer.resetWriterIndex();
            output.writeInt(123);
        }
        long end = System.currentTimeMillis();
        printQps("writeInt", max(), start, end);
    }

    @Test
    public void testReadInt() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            buffer.resetReaderIndex();
            input.readInt();
        }
        long end = System.currentTimeMillis();
        printQps("readInt", max(), start, end);
    }

    @Test
    public void testWriteClass() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            buffer.writerIndex(0);
            output.writeClass(TestStreamable.class);
        }
        long end = System.currentTimeMillis();
        printQps("writeClass", max(), start, end);
    }

    public static class TestStreamable implements Streamable {

        public TestStreamable() {
        }

        public TestStreamable(StreamInput stream) throws IOException {
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {

        }
    }
}
