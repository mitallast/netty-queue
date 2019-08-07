package org.mitallast.queue.transport.netty;

import com.typesafe.config.ConfigFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.security.ECDHFlow;
import org.mitallast.queue.security.SecurityModule;
import org.mitallast.queue.security.SecurityService;

public class ECDHCodecTest extends BaseTest {

    static {
        new SecurityModule();
        Codec.Companion.register(124, TestStreamable.class, TestStreamable.codec);
    }

    @Test
    public void testMessage() throws Exception {
        var config = ConfigFactory.load();
        var securityService = new SecurityService(config);
        var channel = new EmbeddedChannel(
            new CodecEncoder(),
            new CodecDecoder(logging),
            new ECDHNewEncoder(),
            new ECDHCodecDecoder()
        );
        var ecdh1 = securityService.ecdh();
        var ecdh2 = securityService.ecdh();
        channel.attr(ECDHFlow.Companion.getKey()).set(ecdh1);

        var request = ecdh1.requestStart();
        ecdh2.keyAgreement(request);
        assert ecdh2.isAgreement();
        var response = ecdh2.responseStart();
        ecdh1.keyAgreement(response);
        assert ecdh1.isAgreement();

        var msg = new TestStreamable(123123);
        channel.writeOneOutbound(msg);
        channel.writeOneOutbound(msg);
        channel.writeOneOutbound(msg);
        channel.writeOneOutbound(msg);
        channel.flushOutbound();
        ByteBuf outbound = channel.readOutbound();
        logger.info("outbound: {}", outbound);

        channel.writeInbound(outbound);
        var inbound1 = channel.readInbound();
        var inbound2 = channel.readInbound();
        var inbound3 = channel.readInbound();
        var inbound4 = channel.readInbound();
        logger.info("inbound: {}", inbound1);
        logger.info("inbound: {}", inbound2);
        logger.info("inbound: {}", inbound3);
        logger.info("inbound: {}", inbound4);

        Assert.assertEquals(msg, inbound1);
        Assert.assertEquals(msg, inbound2);
        Assert.assertEquals(msg, inbound3);
        Assert.assertEquals(msg, inbound4);
    }

//    @Test
//    public void testMessageEncodeBenchmark() throws Exception {
//        var encoder = new ECDHCodecEncoder();
//        var decoder = new ECDHCodecDecoder();
//
//        ByteBuf buffer = Unpooled.directBuffer(1024);
//        buffer.markWriterIndex();
//
//        TestStreamable message = new TestStreamable(123123);
//
//        int max = 100000000;
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < max; i++) {
//            buffer.resetWriterIndex();
//            encoder.encode(null, message, buffer);
//        }
//        long end = System.currentTimeMillis();
//        printQps("encode", max, start, end);
//    }
//
//    @Test
//    public void testMessageDecodeBenchmark() throws Exception {
//        var encoder = new ECDHCodecEncoder();
//        var decoder = new ECDHCodecDecoder();
//
//        ByteBuf buffer = Unpooled.directBuffer(1024);
//        encoder.encode(null, new TestStreamable(123123), buffer);
//
//        ArrayList<Object> output = new ArrayList<>(1);
//
//        int max = 100000000;
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < max; i++) {
//            buffer.readerIndex(0);
//            decoder.decode(null, buffer, output);
//            output.clear();
//        }
//        long end = System.currentTimeMillis();
//        printQps("decode", max, start, end);
//    }

    public static class TestStreamable implements Message {
        public static final Codec<TestStreamable> codec = Codec.Companion.of(
            TestStreamable::new,
            TestStreamable::value,
            Codec.Companion.longCodec()
        );

        private final long value;

        public TestStreamable(long value) {
            this.value = value;
        }

        public long value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestStreamable that = (TestStreamable) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }
    }
}
