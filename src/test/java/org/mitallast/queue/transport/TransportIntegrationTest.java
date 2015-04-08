package org.mitallast.queue.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.mitallast.queue.action.queue.enqueue.EnQueueAction;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queue.enqueue.EnQueueResponse;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.common.stream.ByteBufStreamOutput;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.transport.client.TransportClient;
import org.mitallast.queue.transport.transport.TransportFrame;

import java.util.ArrayList;
import java.util.List;

public class TransportIntegrationTest extends BaseQueueTest {

    @Override
    protected int max() {
        return super.max() * 10;
    }

    @Test
    public void testEnqueue() throws Exception {
        createQueue();
        assertQueueEmpty();

        TransportClient client = new TransportClient(settings());
        client.start();

        EnQueueRequest enQueueRequest = new EnQueueRequest();
        enQueueRequest.setQueue(queueName());
        enQueueRequest.setMessage(new QueueMessage("Hello world"));

        ByteBuf buffer = Unpooled.buffer();
        try (StreamOutput output = new ByteBufStreamOutput(buffer)) {
            output.writeInt(EnQueueAction.ACTION_ID);
            enQueueRequest.writeTo(output);
        }
        TransportFrame request = TransportFrame.of(1l, buffer);
        SmartFuture<TransportFrame> send = client.send(request);
        client.flush();
        TransportFrame response = send.get();

        try (StreamInput streamInput = response.inputStream()) {
            EnQueueResponse enQueueResponse = new EnQueueResponse();
            enQueueResponse.readFrom(streamInput);

            assert enQueueResponse.getUUID() != null;
            assert enQueueResponse.getUUID().getMostSignificantBits() != 0;
            assert enQueueResponse.getUUID().getLeastSignificantBits() != 0;
        }
    }

    @Test
    public void test() throws Exception {
        TransportClient client = new TransportClient(settings());
        client.start();

        List<SmartFuture<TransportFrame>> futures = new ArrayList<>(max());
        List<TransportFrame> frames = new ArrayList<>(max());
        for (long i = 0; i < max(); i++) {
            frames.add(TransportFrame.of(i));
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            futures.add(client.send(frames.get(i)));
        }
        client.flush();
        for (SmartFuture<TransportFrame> future : futures) {
            TransportFrame frame = future.get();
        }

        long end = System.currentTimeMillis();
        client.stop();
        printQps("send", max(), start, end);
    }

    @Test
    public void testConcurrent() throws Exception {
        long start = System.currentTimeMillis();
        List<SmartFuture<TransportFrame>> futures = new ArrayList<>(total());
        List<TransportFrame> frames = new ArrayList<>(total());
        for (int i = 0; i < total(); i++) {
            frames.add(TransportFrame.of(i));
        }

        TransportClient[] clients = new TransportClient[concurrency()];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new TransportClient(settings());
            clients[i].start();
        }

        executeConcurrent((t, c) -> {
            TransportClient client = clients[t];
            for (int i = t; i < total(); i += c) {
                futures.add(client.send(frames.get(i)));
            }
            client.flush();
            for (int i = t; i < max(); i += c) {
                futures.get(i).get();
            }
        });
        long end = System.currentTimeMillis();
        for (TransportClient client : clients) {
            client.stop();
        }
        printQps("send concurrent", total(), start, end);
    }
}
