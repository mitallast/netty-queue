package org.mitallast.queue.stomp;

import io.netty.handler.codec.stomp.DefaultStompFrame;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.handler.codec.stomp.StompHeaders;
import org.junit.After;
import org.junit.Assert;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class StompIntegrationTest extends BaseQueueTest {

    private static final String QUEUE = "my_queue";
    private final List<StompClient> stompClients = new ArrayList<>();

    public synchronized StompClient stompClient() {
        StompClient stompClient = new StompClient(ImmutableSettings.builder()
                .put("host", "127.0.0.1")
                .put("port", 9080)
                .build());

        stompClient.start();
        stompClients.add(stompClient);
        return stompClient;
    }

    @After
    public void tearDownClient() throws Exception {
        for (StompClient client : stompClients) {
            client.stop();
        }
    }

    @Test
    public void testSingleThread() throws Exception {
        StompFrame connect = new DefaultStompFrame(StompCommand.CONNECT);
        connect.headers().set(StompHeaders.ACCEPT_VERSION, "1.2");
        connect.headers().set(StompHeaders.RECEIPT, "connect");
        StompClient stompClient = stompClient();
        StompFrame connectResponse = stompClient.send(connect).get();
        assert connectResponse.command().equals(StompCommand.CONNECTED);
        assert connectResponse.headers().get(StompHeaders.RECEIPT_ID).equals("connect");

        client().queues().createQueue(new CreateQueueRequest(QUEUE, ImmutableSettings.builder().build())).get();
        QueueStatsResponse response = client().queue().queueStatsRequest(new QueueStatsRequest(QUEUE)).get();
        assert response.getStats().getSize() == 0;

        final int max = 50000;

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

        final int concurrency = 24;
        final int max = 10000;

        final StompClient stompClient = stompClient();
        StompFrame connect = new DefaultStompFrame(StompCommand.CONNECT);
        connect.headers().set(StompHeaders.ACCEPT_VERSION, "1.2");
        connect.headers().set(StompHeaders.RECEIPT, "connect");

        StompFrame connectResponse = stompClient.send(connect).get();
        assert connectResponse.command().equals(StompCommand.CONNECTED);
        assert connectResponse.headers().get(StompHeaders.RECEIPT_ID).equals("connect");

        // warm up
        for (int i = 0; i < concurrency; i++) {
            send(max);
        }

        final ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
        final List<Future> futures = new ArrayList<>(concurrency);

        long start = System.currentTimeMillis();
        for (int i = 0; i < concurrency; i++) {
            Future future = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        StompFrame connect = new DefaultStompFrame(StompCommand.CONNECT);
                        connect.headers().set(StompHeaders.ACCEPT_VERSION, "1.2");
                        connect.headers().set(StompHeaders.RECEIPT, "connect");

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
        final StompClient stompClient = stompClient();
        StompFrame connect = new DefaultStompFrame(StompCommand.CONNECT);
        connect.headers().set(StompHeaders.ACCEPT_VERSION, "1.2");
        connect.headers().set(StompHeaders.RECEIPT, "connect");

        final Map<String, Future<StompFrame>> futures = new HashMap<>(max);
        byte[] data = "Hello world".getBytes();
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
        for (Map.Entry<String, Future<StompFrame>> futureEntry : futures.entrySet()) {
            StompFrame sendResponse = futureEntry.getValue().get();
            Assert.assertEquals(StompCommand.RECEIPT, sendResponse.command());
            Assert.assertEquals(futureEntry.getKey(), sendResponse.headers().get(StompHeaders.RECEIPT_ID));
        }
    }
}
