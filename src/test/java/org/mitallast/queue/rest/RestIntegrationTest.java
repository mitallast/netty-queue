package org.mitallast.queue.rest;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.junit.Test;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.rest.transport.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RestIntegrationTest extends BaseQueueTest {

    private static final String QUEUE = "my_queue";

    @Test
    public void testSingleThread() throws Exception {
        client().queues().createQueue(new CreateQueueRequest(QUEUE, ImmutableSettings.builder().build())).get();
        QueueStatsResponse response = client().queue().queueStatsRequest(new QueueStatsRequest(QUEUE)).get();
        assert response.getStats().getSize() == 0;

        final int max = 20000;

        // warm up
        for (int i = 0; i < 2; i++) {
            send(max);
        }

        long start = System.currentTimeMillis();
        send(max);
        long end = System.currentTimeMillis();

        System.out.println("total " + (end - start) + "ms");
        //noinspection NumericOverflow
        System.out.println((max * 1000 / (end - start)) + " q/s");
    }

    @Test
    public void testMultiThread() throws Exception {
        client().queues().createQueue(new CreateQueueRequest(QUEUE, ImmutableSettings.builder().build())).get();
        QueueStatsResponse response = client().queue().queueStatsRequest(new QueueStatsRequest(QUEUE)).get();
        assert response.getStats().getSize() == 0;

        final int max = 10000;

        // warm up
        for (int i = 0; i < 2; i++) {
            send(max);
        }

        final int concurrency = 24;
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
        final List<Future> futures = new ArrayList<>(concurrency);

        long start = System.currentTimeMillis();
        for (int i = 0; i < concurrency; i++) {
            Future future = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        send(max);
                    } catch (Exception e) {
                        assert false : e;
                    }
                }
            });
            futures.add(future);
        }
        for (Future future : futures) {
            future.get();
        }
        long end = System.currentTimeMillis();

        System.out.println("total " + (end - start) + "ms");
        //noinspection NumericOverflow
        System.out.println((max * concurrency * 1000 / (end - start)) + " q/s");
    }

    private void send(int max) throws Exception {
        RestClient restClient = new RestClient(settings());
        restClient.start();
        try {
            byte[] bytes = "{\"message\":\"hello world\"}".getBytes();
            List<Future<FullHttpResponse>> futures = new ArrayList<>(max);
            for (int i = 0; i < max; i++) {
                DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                        HttpVersion.HTTP_1_1,
                        HttpMethod.PUT,
                        "/" + QUEUE + "/message",
                        Unpooled.wrappedBuffer(bytes)
                );
                request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, bytes.length);
                futures.add(restClient.send(request));
            }
            restClient.flush();
            for (Future<FullHttpResponse> future : futures) {
                FullHttpResponse response = future.get();
                assert response.status().code() >= 200 : response.status();
                assert response.status().code() < 300 : response.status();
                response.content().release();
            }
        } finally {
            restClient.stop();
        }
    }
}
