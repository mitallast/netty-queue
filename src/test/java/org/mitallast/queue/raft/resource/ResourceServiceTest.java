package org.mitallast.queue.raft.resource;

import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseIntegrationTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.node.InternalNode;
import org.mitallast.queue.raft.state.RaftStateContext;
import org.mitallast.queue.raft.state.RaftStateType;

import java.util.concurrent.TimeUnit;

public class ResourceServiceTest extends BaseIntegrationTest {
    @Test
    public void testReopen() throws Exception {
        Settings settings = settings();

        {
            InternalNode node = createNode(settings);

            // await for leader election
            while (true) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                if (node.injector().getInstance(RaftStateContext.class).getState() == RaftStateType.LEADER) {
                    break;
                }
            }
            Node path = node.injector().getInstance(ResourceService.class).create("path").get();
            Assert.assertNotNull(path);

            node.stop();
            node.close();
        }

        {
            InternalNode node = createNode(ImmutableSettings.builder()
                .put(settings)
                .put("node.name", "node2")
                .build());

            // await for leader election
            while (true) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                if (node.injector().getInstance(RaftStateContext.class).getState() == RaftStateType.LEADER) {
                    break;
                }
            }
            Boolean pathExists = node.injector().getInstance(ResourceService.class).exists("path").get();
            Assert.assertTrue(pathExists);
        }
    }
}
