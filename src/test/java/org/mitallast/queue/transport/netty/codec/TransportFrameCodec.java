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
        streamService.register(TestStreamable.class, TestStreamable::new, 123);
        TransportFrameEncoder encoder = new TransportFrameEncoder(streamService);
        TransportFrameDecoder decoder = new TransportFrameDecoder(streamService);

        ByteBuf buffer = Unpooled.buffer();
        encoder.encode(null, PingTransportFrame.CURRENT, buffer);

        ArrayList<Object> output = new ArrayList<>();
        decoder.decode(null, buffer, output);
        Assert.assertEquals(1, output.size());

        PingTransportFrame decoded = (PingTransportFrame) output.get(0);
        Assert.assertEquals(Version.CURRENT, decoded.version());
    }

    @Test
    public void testRequest() throws Exception {
        StreamService streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        streamService.register(TestStreamable.class, TestStreamable::new, 123);
        TransportFrameEncoder encoder = new TransportFrameEncoder(streamService);
        TransportFrameDecoder decoder = new TransportFrameDecoder(streamService);

        ByteBuf buffer = Unpooled.buffer();
        encoder.encode(null, new RequestTransportFrame(Version.CURRENT, 123, new TestStreamable(123123)), buffer);

        ArrayList<Object> output = new ArrayList<>();
        decoder.decode(null, buffer, output);
        Assert.assertEquals(1, output.size());

        RequestTransportFrame decoded = (RequestTransportFrame) output.get(0);
        Assert.assertEquals(Version.CURRENT, decoded.version());
        Assert.assertEquals(123, decoded.request());
        TestStreamable message = decoded.message();
        Assert.assertEquals(123123, message.value);
    }

    @Test
    public void testMessage() throws Exception {
        StreamService streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        streamService.register(TestStreamable.class, TestStreamable::new, 123);
        TransportFrameEncoder encoder = new TransportFrameEncoder(streamService);
        TransportFrameDecoder decoder = new TransportFrameDecoder(streamService);

        ByteBuf buffer = Unpooled.buffer();
        encoder.encode(null, new MessageTransportFrame(Version.CURRENT, new TestStreamable(123123)), buffer);

        ArrayList<Object> output = new ArrayList<>();
        decoder.decode(null, buffer, output);
        Assert.assertEquals(1, output.size());

        MessageTransportFrame decoded = (MessageTransportFrame) output.get(0);
        Assert.assertEquals(Version.CURRENT, decoded.version());
        TestStreamable message = decoded.message();
        Assert.assertEquals(123123, message.value);
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
