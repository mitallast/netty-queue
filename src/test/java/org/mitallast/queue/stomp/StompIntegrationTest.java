package org.mitallast.queue.stomp;

import io.netty.handler.codec.stomp.DefaultStompFrame;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.handler.codec.stomp.StompHeaders;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.stomp.transport.StompClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class StompIntegrationTest extends BaseStompTest {

    private static final int concurrency = 24;
    private static final int max = 10000;

    @Test
    public void testSingleThread() throws Exception {
        createQueue();
        warmUp();
        long start = System.currentTimeMillis();
        send(max * concurrency);
        long end = System.currentTimeMillis();

        System.out.println("total " + (end - start) + "ms");
        //noinspection NumericOverflow
        System.out.println((max * concurrency * 1000 / (end - start)) + " q/s");
    }

    @Test
    public void testMultiThread() throws Exception {
        createQueue();
        warmUp();
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

    private void warmUp() throws Exception {
        for (int i = 0; i < concurrency; i++) {
            send(max / concurrency);
        }
    }

    private void send(int max) throws Exception {
        StompClient stompClient = createStompClient();

        final Map<String, Future<StompFrame>> futures = new HashMap<>(max);
        byte[] data = "Hello world".getBytes();
        for (int i = 0; i < max; i++) {
            String receipt = UUIDs.generateRandom().toString();
            StompFrame sendRequest = new DefaultStompFrame(StompCommand.SEND);
            sendRequest.headers().set(StompHeaders.DESTINATION, queueName());
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
        stompClient.stop();
    }
}
