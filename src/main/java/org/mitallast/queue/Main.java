package org.mitallast.queue;

import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.node.Node;

import java.io.IOException;

public class Main {
    public static void main(String... args) throws IOException, InterruptedException {

        final Settings settings = ImmutableSettings.builder()
                .put("work_dir", "data")
                .build();
        final Node node = new InternalNode(settings);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                node.close();
            }
        });
    }
}
