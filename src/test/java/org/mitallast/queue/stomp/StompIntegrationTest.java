package org.mitallast.queue.stomp;

import io.netty.handler.codec.stomp.DefaultStompFrame;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.handler.codec.stomp.StompHeaders;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.settings.ImmutableSettings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class StompIntegrationTest extends BaseQueueTest {

    private static final String QUEUE = "my_queue";
    private StompClient stompClient;

    @Before
    public void setUpClient() throws Exception {
        stompClient = new StompClient(ImmutableSettings.builder()
            .put("host", "127.0.0.1")
            .put("port", 9080)
            .put("use_oio", false)
            .build());

        stompClient.start();
    }

    @After
    public void tearDownClient() throws Exception {
        if (stompClient != null) {
            stompClient.stop();
        }
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {

        StompFrame connect = new DefaultStompFrame(StompCommand.CONNECT);
        connect.headers().set(StompHeaders.ACCEPT_VERSION, "1.2");
        connect.headers().set(StompHeaders.RECEIPT, "connect");
        StompFrame connectResponse = stompClient.send(connect).get();
        assert connectResponse.command().equals(StompCommand.CONNECTED);
        assert connectResponse.headers().get(StompHeaders.RECEIPT_ID).equals("connect");

        client().queues().createQueue(new CreateQueueRequest(QUEUE, ImmutableSettings.builder().build())).get();
        QueueStatsResponse response = client().queue().queueStatsRequest(new QueueStatsRequest(QUEUE)).get();
        assert response.getStats().getSize() == 0;

        final int max = 10000;
        final int concurrency = 8;
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrency);

        long start = System.currentTimeMillis();

        try {
            List<Future> futureList = new ArrayList<>(concurrency);
            for (int i = 0; i < concurrency; i++) {
                final Future future = executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        final Map<String, Future<StompFrame>> futures = new HashMap<>(max);
                        byte[] data = "Hello world".getBytes();
                        long start = System.currentTimeMillis();
                        for (int i = 0; i < max; i++) {
                            String receipt = java.util.UUID.randomUUID().toString();
                            StompFrame sendRequest = new DefaultStompFrame(StompCommand.SEND);
                            sendRequest.headers().set(StompHeaders.DESTINATION, QUEUE);
                            sendRequest.headers().set(StompHeaders.CONTENT_TYPE, "text");
                            sendRequest.headers().set(StompHeaders.RECEIPT, receipt);
                            sendRequest.headers().set(StompHeaders.CONTENT_LENGTH, data.length);
                            sendRequest.content().writeBytes(data);
                            futures.put(receipt, stompClient.send(sendRequest));
                        }
                        long end = System.currentTimeMillis();
                        long sendTime = end - start;
                        System.out.println("send " + sendTime + "ms");

                        start = System.currentTimeMillis();
                        try {
                            for (Map.Entry<String, Future<StompFrame>> futureEntry : futures.entrySet()) {
                                StompFrame sendResponse = futureEntry.getValue().get();
                                Assert.assertEquals(StompCommand.RECEIPT, sendResponse.command());
                                Assert.assertEquals(futureEntry.getKey(), sendResponse.headers().get(StompHeaders.RECEIPT_ID));
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            assert false : e;
                        }

                        end = System.currentTimeMillis();
                        long awaitTime = end - start;
                        System.out.println("await " + awaitTime + "ms");

                        System.out.println("total " + (sendTime + awaitTime) + "ms");
                        System.out.println((max * 1000 / (sendTime + awaitTime)) + " q/s");
                    }
                });
                futureList.add(future);
            }
            for (Future future : futureList) future.get();
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }

        long end = System.currentTimeMillis();
        System.out.println("total " + (end - start) + "ms");
        //noinspection NumericOverflow
        System.out.println((max * concurrency * 1000 / (end - start)) + " q/s");
    }
}
