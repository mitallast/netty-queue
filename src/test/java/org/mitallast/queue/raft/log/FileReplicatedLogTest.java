package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.raft.protocol.RaftSnapshot;

import java.io.IOException;

public class FileReplicatedLogTest extends ReplicatedLogTest {

    @Override
    protected ReplicatedLog log() throws IOException {
        Config config = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
                .put("node.name", "test")
                .put("node.path", testFolder.getRoot().getAbsolutePath())
                .put("raft.enabled", true)
                .build());
        FileService fileService = new FileService(config);
        StreamService streamService = new InternalStreamService(config, ImmutableSet.of(
                StreamableRegistry.of(AppendWord.class, AppendWord::new, 10000),
                StreamableRegistry.of(LogEntry.class, LogEntry::new, 10001),
                StreamableRegistry.of(RaftSnapshot.class, RaftSnapshot::new, 10002),
                StreamableRegistry.of(StableClusterConfiguration.class, StableClusterConfiguration::new, 10003),
                StreamableRegistry.of(JointConsensusClusterConfiguration.class, JointConsensusClusterConfiguration::new, 10004)
        ));
        FileReplicatedLogProvider provider = new FileReplicatedLogProvider(config, fileService, streamService);
        return provider.get();
    }
}
