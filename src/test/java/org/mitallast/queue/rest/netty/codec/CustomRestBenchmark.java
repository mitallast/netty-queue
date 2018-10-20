package org.mitallast.queue.rest.netty.codec;

import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Test;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.netty.NettyProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class CustomRestBenchmark extends BaseQueueTest {

    private List<CustomRestClient> clients = new ArrayList<>();
    private ArrayBlockingQueue<CustomRestClient> buffer = new ArrayBlockingQueue<>(64);

    @After
    public void tearDownClient() throws Exception {
        for (CustomRestClient restClient : clients) {
            restClient.stop();
            restClient.close();
        }
    }

    private synchronized CustomRestClient alloc() {
        if (buffer.isEmpty()) {
            CustomRestClient client = new CustomRestClient(
                node().config(),
                node().injector().getInstance(NettyProvider.class)
            );
            client.start();
            clients.add(client);
            return client;
        } else {
            return buffer.poll();
        }
    }

    private synchronized void release(CustomRestClient client) {
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
        int max = 100000;
        warmUp();
        while (!Thread.interrupted()) {
            System.gc();
            Thread.sleep(100);
            logger.info("new epoch");
            CountDownLatch latch = new CountDownLatch(concurrency());
            long start = System.currentTimeMillis();
            for (int i = 0; i < concurrency(); i++) {
                async(() -> send(max, latch));
            }
            latch.await();
            long end = System.currentTimeMillis();
            printQps("send", max * concurrency(), start, end);
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
        CustomRestClient restClient = alloc();
        logger.trace("send");
        CountDownLatch responses = new CountDownLatch(max);
        Consumer<HttpResponse> consumer = response -> {
            response.release();
            responses.countDown();
        };
        for (int i = 0; i < max; i++) {
            HttpRequest request = new HttpRequest(
                HttpMethod.GET,
                HttpVersion.HTTP_1_1,
                AsciiString.of("/_index"),
                HttpHeaders.newInstance(),
                Unpooled.EMPTY_BUFFER
            );
            request.headers().put(HttpHeaderName.CONTENT_LENGTH, AsciiString.of("0"));
            restClient.send(request).thenAccept(consumer);
            if (i % 1000 == 0L) {
                restClient.flush();
            }
        }

        restClient.flush();
        responses.await();
        logger.info("await done: {}", max);
        release(restClient);
        latch.countDown();
    }
}
