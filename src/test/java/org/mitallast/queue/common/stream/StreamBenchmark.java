package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseBenchmark;
import org.mitallast.queue.common.settings.ImmutableSettings;

import java.io.IOException;

public class StreamBenchmark extends BaseBenchmark {

    private final static int loop = 100000;
    private StreamService streamService;
    private ByteBuf buffer;
    private StreamInput input;
    private StreamOutput output;

    @Before
    public void setUp() throws Exception {
        streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        streamService.registerClass(TestStreamable.class, TestStreamable::new, 1);
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
        for (int i = 0; i < loop; i++) {
            buffer.resetWriterIndex();
            output.writeInt(123);
        }
    }

    @Test
    public void testReadInt() throws Exception {
        for (int i = 0; i < loop; i++) {
            buffer.resetReaderIndex();
            input.readInt();
        }
    }

    @Test
    public void testWriteClass() throws Exception {
        for (int i = 0; i < loop; i++) {
            buffer.writerIndex(0);
            output.writeClass(TestStreamable.class);
        }
    }

    public static class TestStreamable implements Streamable {

        @Override
        public void readFrom(StreamInput stream) throws IOException {

        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {

        }
    }
}
