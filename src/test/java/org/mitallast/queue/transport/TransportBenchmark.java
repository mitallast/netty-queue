package org.mitallast.queue.transport;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseQueueTest;

import java.util.concurrent.CountDownLatch;

public class TransportBenchmark extends BaseQueueTest {

    private TransportService transportService;
    private DiscoveryNode member;
    private CountDownLatch countDownLatch;

    @Override
    protected int max() {
        return 2000;
    }

    @Override
    protected Config config() throws Exception {
        Map<String, Object> config = HashMap.of(
                "rest.enabled", false,
                "raft.enabled", false,
                "blob.enabled", false
        );
        return ConfigFactory.parseMap(config.toJavaMap()).withFallback(super.config());
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

    public void handle(TestStreamable streamable) {
        countDownLatch.countDown();
    }

    @Test
    public void test() throws Exception {
        for (int e = 0; e < 10; e++) {
            System.gc();
            countDownLatch = new CountDownLatch(total());
            long start = System.currentTimeMillis();
            for (int i = 0; i < total(); i++) {
                transportService.send(member, new TestStreamable(i));
            }
            countDownLatch.await();
            long end = System.currentTimeMillis();
            printQps("send", total(), start, end);
        }
    }

    @Test
    public void testConcurrent() throws Exception {
        for (int e = 0; e < 10; e++) {
            countDownLatch = new CountDownLatch(total());
            long start = System.currentTimeMillis();
            executeConcurrent((thread, concurrency) -> {
                for (int i = thread; i < total(); i += concurrency) {
                    transportService.send(member, new TestStreamable(i));
                }
            });
            countDownLatch.await();
            long end = System.currentTimeMillis();
            printQps("send concurrent", total(), start, end);
        }
    }
}
