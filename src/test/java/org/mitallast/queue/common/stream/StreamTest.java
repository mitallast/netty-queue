package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.ImmutableSettings;

import java.io.IOException;

public class StreamTest extends BaseTest {

    private StreamService streamService;
    private ByteBuf buffer;
    private StreamInput input;
    private StreamOutput output;

    @Before
    public void setUp() throws Exception {
        streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        streamService.registerClass(TestStreamable.class, 1);
        buffer = Unpooled.buffer();
    }

    @After
    public void tearDown() throws Exception {
        input.close();
        output.close();
        buffer.release();
    }

    @Test
    public void testClass() throws Exception {
        output = streamService.output(buffer);
        output.writeClass(TestStreamable.class);
        output.writeClass(TestStreamable.class);
        input = streamService.input(buffer);
        Class<TestStreamable> streamableClass = input.readClass();
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
