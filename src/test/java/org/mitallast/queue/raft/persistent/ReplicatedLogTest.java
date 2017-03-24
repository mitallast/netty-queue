package org.mitallast.queue.raft.persistent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.proto.ProtoRegistry;
import org.mitallast.queue.common.proto.ProtoService;
import org.mitallast.queue.proto.raft.*;
import org.mitallast.queue.proto.test.AppendWord;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ReplicatedLogTest extends BaseTest {

    private final ProtoService protoService = new ProtoService(ImmutableSet.of(
        new ProtoRegistry(1000, AppendWord.getDescriptor(), AppendWord.parser()),
        new ProtoRegistry(1001, LogEntry.getDescriptor(), LogEntry.parser()),
        new ProtoRegistry(1002, RaftSnapshot.getDescriptor(), RaftSnapshot.parser()),
        new ProtoRegistry(1003, StableClusterConfiguration.getDescriptor(), StableClusterConfiguration.parser()),
        new ProtoRegistry(1004, JointConsensusClusterConfiguration.getDescriptor(), JointConsensusClusterConfiguration.parser())
    ));

    private final long term = 1;
    private final long term0 = 0;
    private final long term1 = 1;
    private final long term2 = 2;
    private final long term3 = 3;
    private final DiscoveryNode node1 = DiscoveryNode.newBuilder().setHost("127.0.0.1").setPort(8900).build();
    private final ClusterConfiguration clusterConf = ClusterConfiguration.newBuilder()
        .setStable(StableClusterConfiguration.newBuilder().build())
        .build();

    private final AppendWord word1 = AppendWord.newBuilder().setMessage("word").build();
    private final AppendWord word2 = AppendWord.newBuilder().setMessage("hello world").build();

    private final LogEntry entry1 = logEntry(word1, term, 1, node1);
    private final LogEntry entry2 = logEntry(word1, term, 2, node1);
    private final LogEntry entry3 = logEntry(word1, term, 3, node1);
    private final LogEntry rewriteEntry1 = logEntry(word2, term, 1, node1);
    private final LogEntry rewriteEntry2 = logEntry(word2, term, 2, node1);
    private final LogEntry rewriteEntry3 = logEntry(word2, term, 3, node1);
    private final LogEntry rewriteEntry4 = logEntry(word2, term, 4, node1);
    private final RaftSnapshot snapshot1 = snapshot(snapshotMeta(term, 1, clusterConf));
    private final RaftSnapshot snapshot2 = snapshot(snapshotMeta(term, 2, clusterConf));
    private final RaftSnapshot snapshot3 = snapshot(snapshotMeta(term, 3, clusterConf));

    private final LogEntry snapshotEntry1 = logEntry(snapshot1, term, 1, node1);
    private final LogEntry snapshotEntry2 = logEntry(snapshot2, term, 2, node1);
    private final LogEntry snapshotEntry3 = logEntry(snapshot3, term, 3, node1);

    private LogEntry logEntry(Message message, long term, long index, DiscoveryNode client) {
        return LogEntry.newBuilder()
            .setCommand(protoService.pack(message))
            .setTerm(term)
            .setIndex(index)
            .setClient(client)
            .build();
    }

    private RaftSnapshotMetadata snapshotMeta(long term, long index, ClusterConfiguration config) {
        return RaftSnapshotMetadata.newBuilder()
            .setLastIncludedTerm(term)
            .setLastIncludedIndex(index)
            .setConfig(config)
            .build();
    }

    private RaftSnapshot snapshot(RaftSnapshotMetadata meta) {
        return RaftSnapshot.newBuilder()
            .setMeta(meta)
            .build();
    }

    private RaftSnapshot snapshot(RaftSnapshotMetadata meta, Message data) {
        return RaftSnapshot.newBuilder()
            .setMeta(meta)
            .addData(protoService.pack(data))
            .build();
    }

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

    private ReplicatedLog log() throws Exception {
        return new FilePersistentService(config(), fileService(), protoService).openLog();
    }

    @Test
    public void testReopen() throws Exception {
        ReplicatedLog origin = log().append(entry1).append(entry2).append(entry3).commit(2).compactWith(snapshot2, node1).commit(3);
        logger.info("origin:   {}", origin);
        origin.close();

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
    public void testContainsEntry() throws Exception {
        Assert.assertTrue(log().append(entry1).contains(entry1));
    }

    @Test
    public void testNotContainsEntry() throws Exception {
        Assert.assertFalse(log().append(entry1).contains(entry2));
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
        RaftSnapshot snapshot = snapshot(snapshotMeta(term2, 2, clusterConf));
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
    public void testCompactEmpty() throws Exception {
        ReplicatedLog compacted = log().compactWith(snapshot1, node1);
        Assert.assertEquals(ImmutableList.of(snapshotEntry1), compacted.entries());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompactWithOldSnapshot() throws Exception {
        log().append(entry1).append(entry2).append(entry3)
            .compactWith(snapshot3, node1)
            .compactWith(snapshot1, node1);
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
            .append(logEntry(word1, term1, 1, node1))
            .append(logEntry(word1, term2, 2, node1))
            .append(logEntry(word1, term3, 3, node1));
        Assert.assertEquals(term0, log.termAt(0));
        Assert.assertEquals(term1, log.termAt(1));
        Assert.assertEquals(term2, log.termAt(2));
        Assert.assertEquals(term3, log.termAt(3));
    }

    @Test
    public void testTermAtAfterCompaction1() throws Exception {
        ReplicatedLog log = log()
            .append(logEntry(word1, term1, 1, node1))
            .append(logEntry(word1, term2, 2, node1))
            .append(logEntry(word1, term3, 3, node1))
            .compactWith(snapshot(snapshotMeta(term1, 1, clusterConf)), node1);
        Assert.assertEquals(term1, log.termAt(1));
        Assert.assertEquals(term2, log.termAt(2));
        Assert.assertEquals(term3, log.termAt(3));
    }

    @Test
    public void testTermAtAfterCompaction2() throws Exception {
        ReplicatedLog log = log()
            .append(logEntry(word1, term1, 1, node1))
            .append(logEntry(word1, term2, 2, node1))
            .append(logEntry(word1, term3, 3, node1))
            .compactWith(snapshot(snapshotMeta(term2, 2, clusterConf)), node1);
        Assert.assertEquals(term2, log.termAt(2));
        Assert.assertEquals(term3, log.termAt(3));
    }

    @Test
    public void testTermAtAfterCompaction3() throws Exception {
        ReplicatedLog log = log()
            .append(logEntry(word1, term1, 1, node1))
            .append(logEntry(word1, term2, 2, node1))
            .append(logEntry(word1, term3, 3, node1))
            .compactWith(snapshot(snapshotMeta(term3, 3, clusterConf)), node1);
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
        final long start = System.currentTimeMillis();
        final long total = 1000000;
        for (int i = 0; i < total; i++) {
            log = log.append(logEntry(word2, term, i, node1));
        }
        final long end = System.currentTimeMillis();
        printQps("append", total, start, end);
    }
}
