package org.mitallast.queue.rest;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.junit.Test;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.rest.transport.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class RestIntegrationTest extends BaseQueueTest {

    @Test
    public void test() throws Exception {
        createQueue();
        assertQueueEmpty();
        warmUp();
        long start = System.currentTimeMillis();
        send(max());
        long end = System.currentTimeMillis();
        printQps("send", max(), start, end);
    }

    @Test
    public void testConcurrent() throws Exception {
        createQueue();
        assertQueueEmpty();
        warmUp();
        long start = System.currentTimeMillis();
        executeConcurrent(() -> {
            try {
                send(max());
            } catch (Exception e) {
                assert false : e;
            }
        });
        long end = System.currentTimeMillis();
        printQps("send", total(), start, end);
    }

    private void warmUp() throws Exception {
        send(total());
    }

    private void send(int max) throws Exception {
        RestClient restClient = new RestClient(settings());
        restClient.start();
        try {
            logger.info("send");
            byte[] bytes = "{\"message\":\"hello world\"}".getBytes();
            List<Future<FullHttpResponse>> futures = new ArrayList<>(max);
            for (int i = 0; i < max; i++) {
                DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1,
                    HttpMethod.PUT,
                    "/" + queueName() + "/message",
                    Unpooled.wrappedBuffer(bytes)
                );
                request.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
                futures.add(restClient.send(request));
            }
            restClient.flush();
            logger.info("await");
            for (Future<FullHttpResponse> future : futures) {
                FullHttpResponse response = future.get();
                assert response.status().code() >= 200 : response.status();
                assert response.status().code() < 300 : response.status();
                response.content().release();
            }
            logger.info("await done");
        } finally {
            restClient.stop();
        }
    }
}
