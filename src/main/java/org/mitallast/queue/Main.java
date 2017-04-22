package org.mitallast.queue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.node.Node;

import java.util.concurrent.CountDownLatch;

public class Main {
    public static final Object mutex = new Object();

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load();
        final Node node = new InternalNode(config);
        node.start();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            node.close();
            countDownLatch.countDown();
        }));
        countDownLatch.await();
    }
}
