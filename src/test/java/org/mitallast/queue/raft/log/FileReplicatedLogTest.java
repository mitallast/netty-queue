package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.*;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.raft.persistent.FilePersistentService;
import org.mitallast.queue.raft.persistent.ReplicatedLog;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class FileReplicatedLogTest extends BaseTest {

    private final Term term0 = new Term(0);
    private final Term term1 = new Term(1);
    private final Term term2 = new Term(2);
    private final Term term3 = new Term(3);
    private final DiscoveryNode node1 = new DiscoveryNode("127.0.0.1", 8900);
    private final StableClusterConfiguration clusterConf = new StableClusterConfiguration();
    private Term term = term1;
    private final LogEntry entry1 = new LogEntry(new AppendWord("word"), term, 1, node1);
    private final LogEntry entry2 = new LogEntry(new AppendWord("word"), term, 2, node1);
    private final LogEntry entry3 = new LogEntry(new AppendWord("word"), term, 3, node1);
    private final LogEntry rewriteEntry1 = new LogEntry(new AppendWord("rewrite"), term, 1, node1);
    private final LogEntry rewriteEntry2 = new LogEntry(new AppendWord("rewrite"), term, 2, node1);
    private final LogEntry rewriteEntry3 = new LogEntry(new AppendWord("rewrite"), term, 3, node1);
    private final LogEntry rewriteEntry4 = new LogEntry(new AppendWord("rewrite"), term, 4, node1);
    private final RaftSnapshot snapshot1 = new RaftSnapshot(new RaftSnapshotMetadata(term, 1, clusterConf), null);
    private final RaftSnapshot snapshot2 = new RaftSnapshot(new RaftSnapshotMetadata(term, 2, clusterConf), null);
    private final RaftSnapshot snapshot3 = new RaftSnapshot(new RaftSnapshotMetadata(term, 3, clusterConf), null);

    private final LogEntry snapshotEntry1 = new LogEntry(snapshot1, term, 1, node1);
    private final LogEntry snapshotEntry2 = new LogEntry(snapshot2, term, 2, node1);
    private final LogEntry snapshotEntry3 = new LogEntry(snapshot3, term, 3, node1);

    private Config config() {
        return ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
            .put("node.name", "test")
            .put("node.path", testFolder.getRoot().getAbsolutePath())
            .put("raft.enabled", true)
            .build());
    }

    private FileService fileService() throws Exception {
        return new FileService(config());
    }

    private StreamService streamService() throws Exception {
        return new InternalStreamService(config(), ImmutableSet.of(
            StreamableRegistry.of(AppendWord.class, AppendWord::new, 10000),
            StreamableRegistry.of(LogEntry.class, LogEntry::new, 10001),
            StreamableRegistry.of(RaftSnapshot.class, RaftSnapshot::new, 10002),
            StreamableRegistry.of(StableClusterConfiguration.class, StableClusterConfiguration::new, 10003),
            StreamableRegistry.of(JointConsensusClusterConfiguration.class, JointConsensusClusterConfiguration::new, 10004)
        ));
    }

    private ReplicatedLog log() throws Exception {
        return new FilePersistentService(config(), fileService(), streamService()).openLog();
    }

    @Test
    public void testReopen() throws Exception {
        ReplicatedLog origin = log().append(entry1).append(entry2).append(entry3).commit(2).compactWith(snapshot2, node1).commit(3);
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

    @Test
    public void testAddFirstEntry() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry1), log().append(entry1).entries());
    }

    @Test
    public void testAddSecondEntry() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry1, entry2), log().append(entry1).append(entry2).entries());
    }

    @Test
    public void testMatchNextEntry() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry1, entry3), log().append(entry1).append(entry3).entries());
    }

    @Test
    public void testReturn1NextIndexIfEmpty() throws Exception {
        Assert.assertEquals(1, log().nextIndex());
    }

    @Test
    public void testReturn2NextIndexIfContainsEntry1() throws Exception {
        Assert.assertEquals(2, log().append(entry1).nextIndex());
    }

    @Test
    public void testReturn3NextIndexIfContainsEntry2() throws Exception {
        Assert.assertEquals(3, log().append(entry1).append(entry2).nextIndex());
    }

    @Test
    public void testReturn4NextIndexIfContainsEntry3() throws Exception {
        Assert.assertEquals(4, log().append(entry1).append(entry2).append(entry3).nextIndex());
    }

    @Test
    public void testPrevIndex0IfEmpty() throws Exception {
        Assert.assertEquals(0, log().prevIndex());
    }

    @Test
    public void testPrevIndex0IfContainsEntry1() throws Exception {
        Assert.assertEquals(0, log().append(entry1).prevIndex());
    }

    @Test
    public void testPrevIndex1IfContainsEntry2() throws Exception {
        Assert.assertEquals(1, log().append(entry1).append(entry2).prevIndex());
    }

    @Test
    public void testPrevIndex2IfContainsEntry3() throws Exception {
        Assert.assertEquals(2, log().append(entry1).append(entry2).append(entry3).prevIndex());
    }

    @Test
    public void testNextEntriesLowerBound0() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry1, entry2, entry3), log().append(entry1).append(entry2).append(entry3).entriesBatchFrom(1, 3));
    }

    @Test
    public void testNextEntriesLowerBound1() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry2, entry3), log().append(entry1).append(entry2).append(entry3).entriesBatchFrom(2, 3));
    }

    @Test
    public void testNextEntriesLowerBound2() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry3), log().append(entry1).append(entry2).append(entry3).entriesBatchFrom(3, 3));
    }

    @Test
    public void testNextEntriesLowerBound3() throws Exception {
        Assert.assertEquals(ImmutableList.of(), log().append(entry1).append(entry2).append(entry3).entriesBatchFrom(4, 3));
    }

    @Test
    public void testContainsMatchingEntry0IfEmpty() throws Exception {
        Assert.assertTrue(log().containsMatchingEntry(term0, 0));
    }

    @Test
    public void testContainsMatchingEntry1IfEmpty() throws Exception {
        Assert.assertFalse(log().containsMatchingEntry(term0, 1));
    }

    @Test
    public void testContainsMatchingEntry3IfNotEmpty() throws Exception {
        Assert.assertTrue(log().append(entry1).append(entry2).append(entry3).containsMatchingEntry(term, 3));
    }

    @Test
    public void testContainsMatchingEntry2IfNotEmpty() throws Exception {
        Assert.assertTrue(log().append(entry1).append(entry2).append(entry3).containsMatchingEntry(term, 2));
    }

    @Test
    public void testContainsMatchingEntry1IfNotEmpty() throws Exception {
        Assert.assertTrue(log().append(entry1).append(entry2).append(entry3).containsMatchingEntry(term, 1));
    }

    @Test
    public void testBetween1and1empty() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry1), log().append(entry1).append(entry2).append(entry3).slice(1, 1));
    }

    @Test
    public void testBetween1and2entry2() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry1, entry2), log().append(entry1).append(entry2).append(entry3).slice(1, 2));
    }

    @Test
    public void testBetween1and3entry3() throws Exception {
        Assert.assertEquals(ImmutableList.of(entry1, entry2, entry3), log().append(entry1).append(entry2).append(entry3).slice(1, 3));
    }

    @Test
    public void testCommitIndexIfEmpty() throws Exception {
        Assert.assertEquals(0, log().committedIndex());
    }

    @Test
    public void testNotContainsEntry1ifEmpty() throws Exception {
        Assert.assertFalse(log().containsEntryAt(1));
    }

    @Test
    public void testContainsEntry1ifEntry1() throws Exception {
        Assert.assertTrue(log().append(entry1).containsEntryAt(1));
    }

    @Test
    public void testCompactLog() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).append(entry3).compactWith(snapshot3, node1);
        Assert.assertEquals(ImmutableList.of(snapshotEntry3), compacted.entries());
        Assert.assertTrue(compacted.hasSnapshot());
        Assert.assertEquals(snapshot3, compacted.snapshot());
    }

    @Test
    public void testContainsMatchingEntryAfterCompaction1() throws Exception {
        ReplicatedLog compacted = log().append(entry1).compactWith(snapshot1, node1);
        Assert.assertTrue(compacted.containsMatchingEntry(term, 1));
    }

    @Test
    public void testContainsMatchingEntry1AfterCompaction2() throws Exception {
        RaftSnapshot snapshot = new RaftSnapshot(new RaftSnapshotMetadata(term2, 2, clusterConf), null);
        ReplicatedLog compacted = log().append(entry1).compactWith(snapshot, node1);
        Assert.assertTrue(compacted.containsMatchingEntry(term2, 2));
    }

    @Test
    public void testContainsMatchingEntryAfterCompaction2() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).compactWith(snapshot2, node1);
        Assert.assertTrue(compacted.containsMatchingEntry(term, 2));
    }

    @Test
    public void testContainsMatchingEntryAfterCompaction3() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).append(entry3).compactWith(snapshot3, node1);
        Assert.assertTrue(compacted.containsMatchingEntry(term, 3));
    }

    @Test
    public void testContainsEntry1AfterCompaction1() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).append(entry3).compactWith(snapshot1, node1);
        Assert.assertEquals(ImmutableList.of(snapshotEntry1, entry2, entry3), compacted.entries());
        Assert.assertTrue(compacted.containsEntryAt(1));
        Assert.assertTrue(compacted.containsEntryAt(2));
        Assert.assertTrue(compacted.containsEntryAt(3));
    }

    @Test
    public void testContainsEntry1AfterCompaction2() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).append(entry3).compactWith(snapshot2, node1);
        logger.info("compacted: {}", compacted);
        Assert.assertEquals(ImmutableList.of(snapshotEntry2, entry3), compacted.entries());
        Assert.assertFalse(compacted.containsEntryAt(1));
        Assert.assertTrue(compacted.containsEntryAt(2));
        Assert.assertTrue(compacted.containsEntryAt(3));
    }

    @Test
    public void testContainsEntry1AfterCompaction3() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).append(entry3).compactWith(snapshot3, node1);
        Assert.assertEquals(ImmutableList.of(snapshotEntry3), compacted.entries());
        Assert.assertFalse(compacted.containsEntryAt(1));
        Assert.assertFalse(compacted.containsEntryAt(2));
        Assert.assertTrue(compacted.containsEntryAt(3));
    }

    @Test
    public void testLastTermAfterCompaction1() throws Exception {
        ReplicatedLog compacted = log().append(entry1).compactWith(snapshot1, node1);
        Assert.assertEquals(Optional.of(term), compacted.lastTerm());
    }

    @Test
    public void testLastTermAfterCompaction2() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).compactWith(snapshot1, node1);
        Assert.assertEquals(Optional.of(term), compacted.lastTerm());
    }

    @Test
    public void testLastTermAfterCompaction3() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).append(entry3).compactWith(snapshot1, node1);
        Assert.assertEquals(Optional.of(term), compacted.lastTerm());
    }

    @Test
    public void testLastIndexAfterCompaction1() throws Exception {
        ReplicatedLog compacted = log().append(entry1).compactWith(snapshot1, node1);
        Assert.assertEquals(1, compacted.lastIndex());
    }

    @Test
    public void testLastIndexAfterCompaction2() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).compactWith(snapshot2, node1);
        Assert.assertEquals(2, compacted.lastIndex());
    }

    @Test
    public void testLastIndexAfterCompaction3() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).append(entry3).compactWith(snapshot3, node1);
        Assert.assertEquals(3, compacted.lastIndex());
    }

    @Test
    public void testEntriesBatchFrom1AfterCompaction1() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).append(entry3).compactWith(snapshot1, node1);
        Assert.assertEquals(ImmutableList.of(snapshotEntry1, entry2, entry3), compacted.entriesBatchFrom(1, 3));
    }

    @Test
    public void testEntriesBatchFrom1AfterCompaction2() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).append(entry3).compactWith(snapshot1, node1);
        Assert.assertEquals(ImmutableList.of(entry2, entry3), compacted.entriesBatchFrom(2, 3));
    }

    @Test
    public void testEntriesBatchFrom1AfterCompaction3() throws Exception {
        ReplicatedLog compacted = log().append(entry1).append(entry2).append(entry3).compactWith(snapshot1, node1);
        Assert.assertEquals(ImmutableList.of(entry3), compacted.entriesBatchFrom(3, 3));
    }

    @Test
    public void testCommittedEntriesIfEmpty() throws Exception {
        ReplicatedLog log = log();
        Assert.assertEquals(0, log.committedEntries());
    }

    @Test
    public void testCommittedEntriesIfAppend1Entry() throws Exception {
        ReplicatedLog log = log().append(entry1);
        Assert.assertEquals(0, log.committedEntries());
    }

    @Test
    public void testCommittedEntriesIfAppend2Entry() throws Exception {
        ReplicatedLog log = log().append(entry1).append(entry2);
        Assert.assertEquals(0, log.committedEntries());
    }

    @Test
    public void testCommittedEntriesIfAppend3Entry() throws Exception {
        ReplicatedLog log = log().append(entry1).append(entry2).append(entry3);
        Assert.assertEquals(0, log.committedEntries());
    }

    @Test
    public void testCommittedEntriesIfAppend3EntryAndCommit1() throws Exception {
        ReplicatedLog log = log().append(entry1).append(entry2).append(entry3).commit(1);
        Assert.assertEquals(1, log.committedEntries());
    }

    @Test
    public void testCommittedEntriesIfAppend3EntryAndCommit2() throws Exception {
        ReplicatedLog log = log().append(entry1).append(entry2).append(entry3).commit(2);
        Assert.assertEquals(2, log.committedEntries());
    }

    @Test
    public void testCommittedEntriesIfAppend3EntryAndCommit3() throws Exception {
        ReplicatedLog log = log().append(entry1).append(entry2).append(entry3).commit(3);
        Assert.assertEquals(3, log.committedEntries());
    }

    @Test
    public void testAppendWithIndex0() throws Exception {
        ReplicatedLog prev = log().append(entry1).append(entry2).append(entry3);
        ReplicatedLog rewrite = prev.append(ImmutableList.of(rewriteEntry1), 0);

        Assert.assertEquals(ImmutableList.of(rewriteEntry1), rewrite.entries());
        Assert.assertEquals(prev.committedIndex(), rewrite.committedIndex());
    }

    @Test
    public void testAppendWithIndex1() throws Exception {
        ReplicatedLog prev = log().append(entry1).append(entry2).append(entry3);
        ReplicatedLog rewrite = prev.append(ImmutableList.of(rewriteEntry2), 1);

        Assert.assertEquals(ImmutableList.of(entry1, rewriteEntry2), rewrite.entries());
        Assert.assertEquals(prev.committedIndex(), rewrite.committedIndex());
    }

    @Test
    public void testAppendWithIndex2() throws Exception {
        ReplicatedLog prev = log().append(entry1).append(entry2).append(entry3);
        ReplicatedLog rewrite = prev.append(ImmutableList.of(rewriteEntry3), 2);

        Assert.assertEquals(ImmutableList.of(entry1, entry2, rewriteEntry3), rewrite.entries());
        Assert.assertEquals(prev.committedIndex(), rewrite.committedIndex());
    }

    @Test
    public void testAppendWithIndex3() throws Exception {
        ReplicatedLog prev = log().append(entry1).append(entry2).append(entry3);
        ReplicatedLog rewrite = prev.append(ImmutableList.of(rewriteEntry4), 3);

        Assert.assertEquals(ImmutableList.of(entry1, entry2, entry3, rewriteEntry4), rewrite.entries());
        Assert.assertEquals(prev.committedIndex(), rewrite.committedIndex());
    }

    @Test
    public void testTermAt() throws Exception {
        ReplicatedLog log = log()
            .append(new LogEntry(new AppendWord("word"), term1, 1, node1))
            .append(new LogEntry(new AppendWord("word"), term2, 2, node1))
            .append(new LogEntry(new AppendWord("word"), term3, 3, node1));
        Assert.assertEquals(term0, log.termAt(0));
        Assert.assertEquals(term1, log.termAt(1));
        Assert.assertEquals(term2, log.termAt(2));
        Assert.assertEquals(term3, log.termAt(3));
    }

    @Test
    public void testTermAtAfterCompaction1() throws Exception {
        ReplicatedLog log = log()
            .append(new LogEntry(new AppendWord("word"), term1, 1, node1))
            .append(new LogEntry(new AppendWord("word"), term2, 2, node1))
            .append(new LogEntry(new AppendWord("word"), term3, 3, node1))
            .compactWith(new RaftSnapshot(new RaftSnapshotMetadata(term1, 1, clusterConf), null), node1);
        Assert.assertEquals(term1, log.termAt(1));
        Assert.assertEquals(term2, log.termAt(2));
        Assert.assertEquals(term3, log.termAt(3));
    }

    @Test
    public void testTermAtAfterCompaction2() throws Exception {
        ReplicatedLog log = log()
            .append(new LogEntry(new AppendWord("word"), term1, 1, node1))
            .append(new LogEntry(new AppendWord("word"), term2, 2, node1))
            .append(new LogEntry(new AppendWord("word"), term3, 3, node1))
            .compactWith(new RaftSnapshot(new RaftSnapshotMetadata(term2, 2, clusterConf), null), node1);
        Assert.assertEquals(term2, log.termAt(2));
        Assert.assertEquals(term3, log.termAt(3));
    }

    @Test
    public void testTermAtAfterCompaction3() throws Exception {
        ReplicatedLog log = log()
            .append(new LogEntry(new AppendWord("word"), term1, 1, node1))
            .append(new LogEntry(new AppendWord("word"), term2, 2, node1))
            .append(new LogEntry(new AppendWord("word"), term3, 3, node1))
            .compactWith(new RaftSnapshot(new RaftSnapshotMetadata(term3, 3, clusterConf), null), node1);
        Assert.assertEquals(term3, log.termAt(3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnableFindEntryAt() throws Exception {
        log().termAt(123);
    }

    @Test
    public void testToString() throws Exception {
        Assert.assertFalse(log().toString().isEmpty());
    }

    @Test
    public void benchmarkAppend() throws Exception {
        ReplicatedLog log = log();
        AppendWord cmd = new AppendWord("hello world");
        DiscoveryNode client = new DiscoveryNode("localhost", 8800);
        final long start = System.currentTimeMillis();
        final long total = 3000000;
        for (int i = 0; i < total; i++) {
            log = log.append(new LogEntry(cmd, term, i, client));
        }
        final long end = System.currentTimeMillis();
        printQps("append", total, start, end);
    }

    public class AppendWord implements Streamable {
        private final String word;

        public AppendWord(StreamInput stream) throws IOException {
            word = stream.readText();
        }

        public AppendWord(String word) {
            this.word = word;
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
