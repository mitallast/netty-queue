package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;

import java.io.IOException;

public class ReplicatedLogTest extends BaseTest {

    private final Term term = new Term(1);
    private final ReplicatedLog log = new ReplicatedLog();
    private final LogEntry entry1 = new LogEntry(new AppendWord("word"), term, 1);
    private final LogEntry entry2 = new LogEntry(new AppendWord("word"), term, 2);
    private final LogEntry entry3 = new LogEntry(new AppendWord("word"), term, 3);
    private final StableClusterConfiguration clusterConf = new StableClusterConfiguration(0, ImmutableSet.of());
    private final RaftSnapshot snapshot1 = new RaftSnapshot(new RaftSnapshotMetadata(term, 1, clusterConf), null);
    private final RaftSnapshot snapshot2 = new RaftSnapshot(new RaftSnapshotMetadata(term, 2, clusterConf), null);
    private final RaftSnapshot snapshot3 = new RaftSnapshot(new RaftSnapshotMetadata(term, 3, clusterConf), null);
    private final LogEntry snapshotEntry1 = new LogEntry(snapshot1, term, 1);
    private final LogEntry snapshotEntry2 = new LogEntry(snapshot1, term, 2);
    private final LogEntry snapshotEntry3 = new LogEntry(snapshot1, term, 3);

    @Test
    public void testAddFirstEntry() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry1), log.append(entry1).entries());
    }

    @Test
    public void testAddSecondEntry() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry1, entry2), log.append(entry1).append(entry2).entries());
    }

    @Test
    public void testMatchNextEntry() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry1, entry3), log.append(entry1).append(entry3).entries());
    }

    @Test
    public void testReturn1NextIndexIfEmpty() throws Exception {
        Assert.assertEquals(1, log.nextIndex());
    }

    @Test
    public void testReturn2NextIndexIfContainsEntry1() throws Exception {
        Assert.assertEquals(2, log.append(entry1).nextIndex());
    }

    @Test
    public void testReturn3NextIndexIfContainsEntry2() throws Exception {
        Assert.assertEquals(3, log.append(entry1).append(entry2).nextIndex());
    }

    @Test
    public void testReturn4NextIndexIfContainsEntry3() throws Exception {
        Assert.assertEquals(4, log.append(entry1).append(entry2).append(entry3).nextIndex());
    }

    @Test
    public void testPrevIndex0IfEmpty() throws Exception {
        Assert.assertEquals(0, log.prevIndex());
    }

    @Test
    public void testPrevIndex0IfContainsEntry1() throws Exception {
        Assert.assertEquals(0, log.append(entry1).prevIndex());
    }

    @Test
    public void testPrevIndex1IfContainsEntry2() throws Exception {
        Assert.assertEquals(1, log.append(entry1).append(entry2).prevIndex());
    }

    @Test
    public void testPrevIndex2IfContainsEntry3() throws Exception {
        Assert.assertEquals(2, log.append(entry1).append(entry2).append(entry3).prevIndex());
    }

    @Test
    public void testNextEntriesLowerBound0() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry1, entry2, entry3), log.append(entry1).append(entry2).append(entry3).entriesBatchFrom(0));
    }

    private class AppendWord implements Streamable {
        private final String word;

        private AppendWord(String word) {
            this.word = word;
        }

        public String getWord() {
            return word;
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeText(word);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AppendWord that = (AppendWord) o;

            return word.equals(that.word);

        }

        @Override
        public int hashCode() {
            return word.hashCode();
        }
    }
}
