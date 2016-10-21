package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class TransportBenchmark extends BaseQueueTest {

    private TransportChannel channel;
    private CountDownLatch countDownLatch;

    @Override
    protected int max() {
        return 200000;
    }

    @Override
    protected Config config() throws Exception {
        ImmutableMap<String, Object> config = ImmutableMap.<String, Object>builder()
                .put("rest.enabled", false)
                .put("raft.enabled", false)
                .build();
        return ConfigFactory.parseMap(config).withFallback(super.config());
    }

    @Before
    public void setUp() throws Exception {
        StreamService streamService = node().injector().getInstance(StreamService.class);
        streamService.register(TestStreamable.class, TestStreamable::new, 1000000);

        TransportService transportService = node().injector().getInstance(TransportService.class);
        TransportServer transportServer = node().injector().getInstance(TransportServer.class);

        TransportController transportController = node().injector().getInstance(TransportController.class);
        transportController.registerMessageHandler(TestStreamable.class, this::handle);

        HostAndPort address = transportServer.localAddress();
        transportService.connectToNode(address);
        channel = transportService.channel(address);
    }

    public void handle(TransportChannel channel, TestStreamable streamable) {
        countDownLatch.countDown();
    }

    @Test
    public void test() throws Exception {
        warmUp();
        countDownLatch = new CountDownLatch(max());
        long start = System.currentTimeMillis();
        for (int i = 0; i < total(); i++) {
            channel.send(new MessageTransportFrame(Version.CURRENT, new TestStreamable(i)));
        }
        countDownLatch.await();
        long end = System.currentTimeMillis();
        printQps("send", total(), start, end);
    }

    private void warmUp() throws Exception {
        int warmUp = total();
        countDownLatch = new CountDownLatch(warmUp);
        for (int i = 0; i < warmUp; i++) {
            channel.send(new MessageTransportFrame(Version.CURRENT, new TestStreamable(i)));
        }
        countDownLatch.await();
        System.gc();
    }

    private static class TestStreamable implements Streamable {

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

        @Override
        public String toString() {
            return "TestStreamable{" +
                    "value=" + value +
                    '}';
        }
    }
}
