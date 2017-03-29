package org.mitallast.queue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.node.Node;

import java.io.IOException;

public class Main {
    public static final Object mutex = new Object();

    public static void main(String... args) throws Exception {
        Config config = ConfigFactory.load();
        final Node node = new InternalNode(config);
        node.start();

        synchronized (mutex) {
            mutex.wait();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                synchronized (mutex) {
                    mutex.notify();
                }
                node.close();
            } catch (IOException e) {
                e.printStackTrace(System.err);
            }
        }));
    }
}
