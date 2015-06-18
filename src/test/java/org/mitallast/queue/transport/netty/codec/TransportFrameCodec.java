package org.mitallast.queue.transport.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.stream.*;

import java.io.IOException;
import java.util.ArrayList;

public class TransportFrameCodec extends BaseTest {

    @Test
    public void testPing() throws Exception {
        StreamService streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        streamService.registerClass(TestStreamable.class, 123);
        TransportFrameEncoder encoder = new TransportFrameEncoder(streamService);
        TransportFrameDecoder decoder = new TransportFrameDecoder(streamService);

        ByteBuf buffer = Unpooled.buffer();
        encoder.encode(null, TransportFrame.of(Version.CURRENT, 123), buffer);

        ArrayList<Object> output = new ArrayList<>();
        decoder.decode(null, buffer, output);
        Assert.assertEquals(1, output.size());

        TransportFrame decoded = (TransportFrame) output.get(0);
        Assert.assertEquals(Version.CURRENT, decoded.version());
        Assert.assertEquals(123, decoded.request());
    }

    @Test
    public void testStreamable() throws Exception {
        StreamService streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        streamService.registerClass(TestStreamable.class, 123);
        TransportFrameEncoder encoder = new TransportFrameEncoder(streamService);
        TransportFrameDecoder decoder = new TransportFrameDecoder(streamService);

        ByteBuf buffer = Unpooled.buffer();
        encoder.encode(null, StreamableTransportFrame.of(Version.CURRENT, 123, new TestStreamable(123123)), buffer);

        ArrayList<Object> output = new ArrayList<>();
        decoder.decode(null, buffer, output);
        Assert.assertEquals(1, output.size());

        StreamableTransportFrame decoded = (StreamableTransportFrame) output.get(0);
        Assert.assertEquals(Version.CURRENT, decoded.version());
        Assert.assertEquals(123, decoded.request());
        TestStreamable message = decoded.message();
        Assert.assertEquals(123123, message.value);
    }

    public static class TestStreamable implements Streamable {

        private long value;

        public TestStreamable() {
        }

        public TestStreamable(long value) {
            this.value = value;
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            value = stream.readLong();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeLong(value);
        }
    }
}
