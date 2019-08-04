package org.mitallast.queue.rest;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.After;
import org.junit.Test;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.logging.LoggingService;
import org.mitallast.queue.common.netty.NettyProvider;
import org.mitallast.queue.rest.transport.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class RestIntegrationTest extends BaseQueueTest {

    private List<RestClient> clients = new ArrayList<>();
    private ArrayBlockingQueue<RestClient> buffer = new ArrayBlockingQueue<>(64);

    @After
    public void tearDownClient() throws Exception {
        for (RestClient restClient : clients) {
            restClient.stop();
            restClient.close();
        }
    }

    private synchronized RestClient alloc() {
        if (buffer.isEmpty()) {
            RestClient client = new RestClient(
                node().config(),
                node().injector().getInstance(LoggingService.class),
                node().injector().getInstance(NettyProvider.class)
            );
            client.start();
            clients.add(client);
            return client;
        } else {
            return buffer.poll();
        }
    }

    private synchronized void release(RestClient client) {
        buffer.add(client);
    }

    @Test
    public void test() throws Exception {
        warmUp();
        long start = System.currentTimeMillis();
        send(max());
        long end = System.currentTimeMillis();
        printQps("send", max(), start, end);
    }

    @Test
    public void testConcurrent() throws Exception {
        warmUp();
        while (!Thread.interrupted()) {
            System.gc();
            Thread.sleep(100);
            logger.info("new epoch");
            CountDownLatch latch = new CountDownLatch(concurrency());
            long start = System.currentTimeMillis();
            for (int i = 0; i < concurrency(); i++) {
                async(() -> send(max(), latch));
            }
            latch.await();
            long end = System.currentTimeMillis();
            printQps("send", total(), start, end);
        }
    }

    private void warmUp() throws Exception {
        CountDownLatch latch = new CountDownLatch(concurrency());
        for (int i = 0; i < concurrency(); i++) {
            async(() -> send(10000, latch));
        }
        latch.await();
    }

    private void send(int max) throws Exception {
        send(max, new CountDownLatch(1));
    }

    private void send(int max, CountDownLatch latch) throws Exception {
        RestClient restClient = alloc();
        logger.trace("send");
        AtomicLong counter = new AtomicLong();
        CountDownLatch responses = new CountDownLatch(max);
        Consumer<FullHttpResponse> consumer = response -> {
            response.content().release();
            long current = counter.incrementAndGet();
            if (current % 10000 == 0L) {
                logger.info("responses: {}", current);
            }
            responses.countDown();
        };
        for (int i = 0; i < max; i++) {
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "/_index",
                restClient.alloc().buffer()
            );
            restClient.send(request).thenAccept(consumer);
            if (i % 1000 == 0L) {
                restClient.flush();
            }
        }

        restClient.flush();
        logger.trace("await");
        responses.await();
        logger.info("await done: {} of {}", counter.get(), max);
        release(restClient);
        latch.countDown();
    }
}
