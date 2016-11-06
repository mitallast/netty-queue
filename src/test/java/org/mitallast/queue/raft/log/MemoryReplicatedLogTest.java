package org.mitallast.queue.raft.log;

public class MemoryReplicatedLogTest extends ReplicatedLogTest {
    public MemoryReplicatedLogTest() {
        super(new MemoryReplicatedLog());
    }
}
