package org.mitallast.queue;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.node.Node;

import java.io.IOException;

public class Main {
//    public static void main(String... args) throws Exception {
//        Config config = ConfigFactory.load();
//        final Node node = new InternalNode(config);
//        node.start();
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                try {
//                    node.close();
//                } catch (IOException e) {
//                    e.printStackTrace(System.err);
//                }
//            }
//        });
//    }

    public static void main(String... args) throws Exception {

        Config config1 = ConfigFactory.parseMap(ImmutableMap.of(
            "transport.port", 8900,
            "rest.port", 8800,
            "node.name", "node_0"
        )).withFallback(ConfigFactory.load());
        final Node node1 = new InternalNode(config1);
        node1.start();

        Config config2 = ConfigFactory.parseMap(ImmutableMap.of(
            "transport.port", 8901,
            "rest.port", 8801,
            "node.name", "node_1"
        )).withFallback(ConfigFactory.load());
        final Node node2 = new InternalNode(config2);
        node2.start();

        Config config3 = ConfigFactory.parseMap(ImmutableMap.of(
            "transport.port", 8902,
            "rest.port", 8802,
            "node.name", "node_2"
        )).withFallback(ConfigFactory.load());
        final Node node3 = new InternalNode(config3);
        node3.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    node1.close();
                    node2.close();
                    node3.close();
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                }
            }
        });
    }
}
