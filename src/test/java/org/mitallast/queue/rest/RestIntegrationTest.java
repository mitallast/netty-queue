package org.mitallast.queue.rest;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.rest.transport.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class RestIntegrationTest extends BaseQueueTest {

    private List<RestClient> clients;
    private int client;

    @Before
    public void setUpClient() throws Exception {
        client = 0;
        clients = new ArrayList<>();
        for (int i = 0; i <= concurrency(); i++) {
            RestClient restClient = new RestClient(node().config());
            restClient.start();
            clients.add(restClient);
        }
    }

    @After
    public void tearDownClient() throws Exception {
        for (RestClient restClient : clients) {
            restClient.stop();
            restClient.close();
        }
    }

    private synchronized RestClient restClient() {
        return clients.get(client++);
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
        long start = System.currentTimeMillis();
        executeConcurrent(() -> send(max()));
        long end = System.currentTimeMillis();
        printQps("send", total(), start, end);
    }

    private void warmUp() throws Exception {
        send(1000);
    }

    private void send(int max) throws Exception {
        RestClient restClient = restClient();
        logger.info("send");
        List<Future<FullHttpResponse>> futures = new ArrayList<>(max);
        Consumer<FullHttpResponse> consumer = response -> response.content().release();

        for (int i = 0; i < max; i++) {
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "/",
                false
            );
            CompletableFuture<FullHttpResponse> future = restClient.send(request);
            future.thenAccept(consumer);
            futures.add(future);
        }
        logger.info("await");
        for (Future<FullHttpResponse> future : futures) {
            FullHttpResponse response = future.get();
            assert response.status().code() >= 200 : response.status();
            assert response.status().code() < 300 : response.status();
        }
        logger.info("await done");
    }
}
