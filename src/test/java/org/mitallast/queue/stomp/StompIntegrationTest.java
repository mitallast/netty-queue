package org.mitallast.queue.stomp;

import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.handler.codec.stomp.StompHeaders;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.stomp.transport.StompClient;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class StompIntegrationTest extends BaseStompTest {

    @Test
    public void testSingleThread() throws Exception {
        createQueue();
        warmUp();
        long start = System.currentTimeMillis();
        send(total());
        long end = System.currentTimeMillis();
        printQps("send", total(), start, end);
    }

    @Test
    public void testMultiThread() throws Exception {
        createQueue();
        warmUp();
        long start = System.currentTimeMillis();
        executeConcurrent(new Runnable() {
            @Override
            public void run() {
                try {
                    send(max());
                } catch (Exception e) {
                    assert false : e;
                }
            }
        });
        long end = System.currentTimeMillis();
        printQps("send", total(), start, end);
    }

    private void warmUp() throws Exception {
        send(total());
    }

    private void send(int max) throws Exception {
        StompClient stompClient = createStompClient();
        final Map<String, Future<StompFrame>> futures = new HashMap<>(max);
        byte[] data = "Hello world".getBytes();
        for (int i = 0; i < max; i++) {
            StompFrame sendRequest = sendFrame(data);
            futures.put(sendRequest.headers().get(StompHeaders.RECEIPT), stompClient.send(sendRequest, false));
        }
        stompClient.flush();
        for (Map.Entry<String, Future<StompFrame>> futureEntry : futures.entrySet()) {
            StompFrame sendResponse = futureEntry.getValue().get();
            Assert.assertEquals(StompCommand.RECEIPT, sendResponse.command());
            Assert.assertEquals(sendResponse.headers().get(StompHeaders.RECEIPT_ID), futureEntry.getKey());
        }
        stompClient.stop();
    }
}
