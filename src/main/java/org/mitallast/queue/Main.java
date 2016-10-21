package org.mitallast.queue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.node.Node;

import java.io.IOException;

public class Main {
    public static void main(String... args) throws IOException, InterruptedException {
        Config config = ConfigFactory.load();
        final Node node = new InternalNode(config);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    node.close();
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                }
            }
        });
    }
}
