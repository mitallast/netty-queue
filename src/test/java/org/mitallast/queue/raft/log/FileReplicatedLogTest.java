package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.raft.protocol.RaftSnapshot;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class FileReplicatedLogTest extends ReplicatedLogTest {

    protected Config config() {
        return ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
            .put("node.name", "test")
            .put("node.path", testFolder.getRoot().getAbsolutePath())
            .put("raft.enabled", true)
            .build());
    }

    protected FileService fileService() throws Exception {
        return new FileService(config());
    }

    protected StreamService streamService() throws Exception {
        return new InternalStreamService(config(), ImmutableSet.of(
            StreamableRegistry.of(AppendWord.class, AppendWord::new, 10000),
            StreamableRegistry.of(LogEntry.class, LogEntry::new, 10001),
            StreamableRegistry.of(RaftSnapshot.class, RaftSnapshot::new, 10002),
            StreamableRegistry.of(StableClusterConfiguration.class, StableClusterConfiguration::new, 10003),
            StreamableRegistry.of(JointConsensusClusterConfiguration.class, JointConsensusClusterConfiguration::new, 10004)
        ));
    }

    @Override
    protected ReplicatedLog log() throws Exception {
        return new FileReplicatedLogProvider(config(), fileService(), streamService()).get();
    }

    @Test
    public void testReopen() throws Exception {
        ReplicatedLog origin = log().append(entry1).append(entry2).append(entry3).commit(2).compactedWith(snapshot2, node1).commit(3);
        logger.info("origin:   {}", origin);

        ReplicatedLog reopened = log();
        logger.info("reopened: {}", reopened);

        // committed index should not be persistent
        Assert.assertEquals(0, reopened.committedIndex());

        Assert.assertTrue(reopened.hasSnapshot());
        Assert.assertEquals(snapshot2, reopened.snapshot());

        Assert.assertEquals(2, reopened.entries().size());
        Assert.assertEquals(ImmutableList.of(snapshotEntry2, entry3), reopened.entries());

        List<String> files = fileService().resources("raft").map(Path::toString).collect(Collectors.toList());
        logger.info("files: {}", files);
        Assert.assertEquals(2, files.size());
        Assert.assertTrue(files.contains("state.bin"));
        Assert.assertTrue(files.contains("2.log"));
    }
}
