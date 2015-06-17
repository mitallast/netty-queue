package org.mitallast.queue.transport;

import org.junit.Test;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.transport.netty.codec.TransportFrame;

import java.util.ArrayList;
import java.util.List;

public class TransportBenchmark extends BaseQueueTest {

    @Override
    protected int max() {
        return super.max() * 10;
    }

    @Test
    public void test() throws Exception {
        TransportService transportService = node().injector().getInstance(TransportService.class);
        transportService.connectToNode(node().localNode());

        List<SmartFuture<TransportFrame>> futures = new ArrayList<>(max());
        List<TransportFrame> frames = new ArrayList<>(max());
        for (long i = 0; i < max(); i++) {
            frames.add(TransportFrame.of(i));
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            futures.add(transportService.sendRequest(node().localNode(), frames.get(i)));
        }
        for (SmartFuture<TransportFrame> future : futures) {
            TransportFrame frame = future.get();
        }

        long end = System.currentTimeMillis();
        printQps("send", max(), start, end);
    }

    @Test
    public void testConcurrent() throws Exception {
        List<SmartFuture<TransportFrame>> futures = new ArrayList<>(total());
        List<TransportFrame> frames = new ArrayList<>(total());
        for (int i = 0; i < total(); i++) {
            frames.add(TransportFrame.of(i));
        }

        TransportService transportService = node().injector().getInstance(TransportService.class);
        transportService.connectToNode(node().localNode());

        long start = System.currentTimeMillis();
        executeConcurrent((t, c) -> {
            for (int i = t; i < total(); i += c) {
                futures.add(transportService.sendRequest(node().localNode(), frames.get(i)));
            }
            for (int i = t; i < max(); i += c) {
                futures.get(i).get();
            }
        });
        long end = System.currentTimeMillis();
        printQps("send concurrent", total(), start, end);
    }
}
