package org.mitallast.queue.transport.netty.codec;

import com.typesafe.config.ConfigFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.stream.*;

import java.io.IOException;
import java.util.ArrayList;

public class TransportFrameTest extends BaseTest {

    @Test
    public void testPing() throws Exception {
        StreamService streamService = new InternalStreamService(ConfigFactory.defaultReference());
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
    public void testMessage() throws Exception {
        StreamService streamService = new InternalStreamService(ConfigFactory.defaultReference());
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

    @Test
    public void testMessageEncodeBenchmark() throws Exception {
        StreamService streamService = new InternalStreamService(ConfigFactory.defaultReference());
        streamService.register(TestStreamable.class, TestStreamable::new, 123);
        TransportFrameEncoder encoder = new TransportFrameEncoder(streamService);

        ByteBuf buffer = Unpooled.directBuffer(1024);
        buffer.markWriterIndex();

        MessageTransportFrame frame = new MessageTransportFrame(Version.CURRENT, new TestStreamable(123123));

        int max = 100000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < max; i++) {
            buffer.resetWriterIndex();
            encoder.encode(null, frame, buffer);
        }
        long end = System.currentTimeMillis();
        printQps("encode", max, start, end);
    }

    @Test
    public void testMessageDecodeBenchmark() throws Exception {
        StreamService streamService = new InternalStreamService(ConfigFactory.defaultReference());
        streamService.register(TestStreamable.class, TestStreamable::new, 123);
        TransportFrameEncoder encoder = new TransportFrameEncoder(streamService);
        TransportFrameDecoder decoder = new TransportFrameDecoder(streamService);

        ByteBuf buffer = Unpooled.directBuffer(1024);
        encoder.encode(null, new MessageTransportFrame(Version.CURRENT, new TestStreamable(123123)), buffer);

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
