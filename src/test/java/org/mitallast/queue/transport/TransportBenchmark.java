package org.mitallast.queue.transport;

import com.google.common.net.HostAndPort;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.netty.codec.TransportFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TransportBenchmark extends BaseQueueTest {

    private TransportClient client;

    @Override
    protected int max() {
        return super.max() * 10;
    }

    @Override
    protected Settings settings() throws Exception {
        return ImmutableSettings.builder()
            .put(super.settings())
            .put("rest.enabled", false)
            .build();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        TransportService transportService = node().injector().getInstance(TransportService.class);
        HostAndPort address = transportService.localAddress();
        // hack for not equals local node
        HostAndPort localhost = HostAndPort.fromParts("localhost", address.getPort());
        transportService.connectToNode(localhost);
        client = transportService.client(localhost);
    }

    @Test
    public void test() throws Exception {
        List<CompletableFuture<TransportFrame>> futures = new ArrayList<>(max());
        List<TransportFrame> frames = new ArrayList<>(max());
        for (long i = 0; i < max(); i++) {
            frames.add(TransportFrame.of(i));
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            futures.add(client.send(frames.get(i)));
        }
        for (CompletableFuture<TransportFrame> future : futures) {
            future.get();
        }

        long end = System.currentTimeMillis();
        printQps("send", max(), start, end);
    }

    @Test
    public void testConcurrent() throws Exception {
        List<CompletableFuture<TransportFrame>> futures = new ArrayList<>(total());
        List<TransportFrame> frames = new ArrayList<>(total());
        for (int i = 0; i < total(); i++) {
            frames.add(TransportFrame.of(i));
        }

        long start = System.currentTimeMillis();
        executeConcurrent((t, c) -> {
            for (int i = t; i < total(); i += c) {
                futures.add(client.send(frames.get(i)));
            }
            for (int i = t; i < max(); i += c) {
                futures.get(i).get();
            }
        });
        long end = System.currentTimeMillis();
        printQps("send concurrent", total(), start, end);
    }
}
