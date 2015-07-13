package org.mitallast.queue.log;

import org.junit.Test;
import org.mitallast.queue.common.BaseIntegrationTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.log.entry.TextLogEntry;
import org.mitallast.queue.node.InternalNode;

public class LogServiceTest extends BaseIntegrationTest {

    @Test
    public void test() throws Exception {
        InternalNode node = createNode(ImmutableSettings.builder()
            .put(settings())
            .put("raft.enabled", false)
            .put("rest.enabled", false)
            .build());


        LogService logService = node.injector().getInstance(LogService.class);

        Log log = logService.openLog("test log");

        log.appendEntry(TextLogEntry.builder()
            .setIndex(log.nextIndex())
            .setMessage("hello world")
            .build());

        log.appendEntry(TextLogEntry.builder()
            .setIndex(log.nextIndex())
            .setMessage("hello world")
            .build());
    }
}
