package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import javaslang.collection.HashSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

public class StreamBenchmark extends BaseTest {
    private ByteBuf buffer;
    private StreamInput input;
    private StreamOutput output;

    @Override
    protected int max() {
        return 10000000;
    }

    @Before
    public void setUp() throws Exception {
        StreamService streamService = new InternalStreamService(HashSet.of(StreamableRegistry.of(TestStreamable.class, TestStreamable::new, 123))
            .toJavaSet());
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

        public TestStreamable(StreamInput stream) {
        }

        @Override
        public void writeTo(StreamOutput stream) {

        }
    }
}
