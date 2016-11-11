package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;

import java.util.concurrent.CountDownLatch;

public class TransportBenchmark extends BaseQueueTest {

    private TransportService transportService;
    private DiscoveryNode member;
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
            .put("blob.enabled", false)
            .build();
        return ConfigFactory.parseMap(config).withFallback(super.config());
    }

    @Before
    public void setUp() throws Exception {
        transportService = node().injector().getInstance(TransportService.class);
        TransportServer transportServer = node().injector().getInstance(TransportServer.class);

        TransportController transportController = node().injector().getInstance(TransportController.class);
        transportController.registerMessageHandler(TestStreamable.class, this::handle);

        member = transportServer.localNode();
        transportService.connectToNode(member);
    }

    public void handle(TransportChannel channel, TestStreamable streamable) {
        countDownLatch.countDown();
    }

    @Test
    public void test() throws Exception {
        warmUp();
        countDownLatch = new CountDownLatch(total());
        long start = System.currentTimeMillis();
        for (int i = 0; i < total(); i++) {
            transportService.channel(member).send(new MessageTransportFrame(Version.CURRENT, new TestStreamable(i)));
        }
        countDownLatch.await();
        long end = System.currentTimeMillis();
        printQps("send", total(), start, end);
    }

    @Test
    public void testConcurrent() throws Exception {
        warmUp();
        System.gc();
        countDownLatch = new CountDownLatch(total());
        long start = System.currentTimeMillis();
        executeConcurrent((thread, concurrency) -> {
            for (int i = thread; i < total(); i += concurrency) {
                transportService.channel(member).send(new MessageTransportFrame(Version.CURRENT, new TestStreamable(i)));
            }
        });
        countDownLatch.await();
        long end = System.currentTimeMillis();
        printQps("send", total(), start, end);
    }

    private void warmUp() throws Exception {
        int warmUp = total() * 4;
        countDownLatch = new CountDownLatch(warmUp);
        for (int i = 0; i < warmUp; i++) {
            transportService.channel(member).send(new MessageTransportFrame(Version.CURRENT, new TestStreamable(i)));
        }
        countDownLatch.await();
        System.gc();
    }
}
