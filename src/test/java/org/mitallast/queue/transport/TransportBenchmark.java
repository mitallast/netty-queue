package org.mitallast.queue.transport;

import org.junit.Test;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.transport.client.TransportClient;
import org.mitallast.queue.transport.transport.TransportFrame;

import java.util.ArrayList;
import java.util.List;

public class TransportBenchmark extends BaseQueueTest {

    @Override
    protected int max() {
        return super.max() * 10;
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
