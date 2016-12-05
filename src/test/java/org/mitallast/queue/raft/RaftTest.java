package org.mitallast.queue.raft;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.Multibinder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.component.ComponentModule;
import org.mitallast.queue.common.component.LifecycleService;
import org.mitallast.queue.common.file.FileModule;
import org.mitallast.queue.common.stream.StreamModule;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.log.FileReplicatedLogProvider;
import org.mitallast.queue.raft.log.ReplicatedLog;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.LinkedList;
import java.util.concurrent.*;

import static org.mitallast.queue.raft.RaftState.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class RaftTest extends BaseTest {

    private Injector injector;

    @Mock
    private TransportChannel transportChannel1;

    @Mock
    private TransportChannel transportChannel2;

    @Mock
    private TransportChannel transportChannel3;

    @Mock
    private TransportChannel transportChannel4;

    @Mock
    private TransportChannel transportChannel5;

    @Mock
    private TransportService transportService;

    @Mock
    private ClusterDiscovery clusterDiscovery;

    @Mock
    private ResourceFSM resourceFSM;

    private TestRaftContext context;

    private Raft raft;

    private ReplicatedLog log;

    private DiscoveryNode node1 = new DiscoveryNode("localhost", 8801);
    private DiscoveryNode node2 = new DiscoveryNode("localhost", 8802);
    private DiscoveryNode node3 = new DiscoveryNode("localhost", 8803);
    private DiscoveryNode node4 = new DiscoveryNode("localhost", 8804);
    private DiscoveryNode node5 = new DiscoveryNode("localhost", 8805);

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(clusterDiscovery.self()).thenReturn(node1);
        when(clusterDiscovery.discoveryNodes()).thenReturn(ImmutableSet.of());
        when(transportService.channel(node1)).thenReturn(transportChannel1);
        when(transportService.channel(node2)).thenReturn(transportChannel2);
        when(transportService.channel(node3)).thenReturn(transportChannel3);
        when(transportService.channel(node4)).thenReturn(transportChannel4);
        when(transportService.channel(node5)).thenReturn(transportChannel5);

        context = new TestRaftContext();
        Config config = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
            .put("node.name", "test")
            .put("node.path", testFolder.getRoot().getAbsolutePath())
            .put("raft.enabled", true)
            .put("raft.bootstrap", true)
            .build()).withFallback(ConfigFactory.defaultReference());
        injector = Guice.createInjector(
            new ComponentModule(config),
            new StreamModule(),
            new FileModule(),
            new TestRaftModule()
        );
        log = injector.getInstance(ReplicatedLog.class);
        injector.getInstance(LifecycleService.class).start();
    }

    private void appendClusterConf() {
        log = log.append(new LogEntry(new StableClusterConfiguration(node1, node2, node3), new Term(1), 1, node1)).commit(1);
    }

    private void appendBigClusterConf() {
        log = log.append(new LogEntry(new StableClusterConfiguration(node1, node2, node3, node4, node5), new Term(1), 1, node1)).commit(1);
    }

    private void start() throws Exception {
        raft = new Raft(
            injector.getInstance(Config.class),
            injector.getInstance(TransportService.class),
            injector.getInstance(TransportController.class),
            injector.getInstance(ClusterDiscovery.class),
            log,
            resourceFSM,
            context
        );
        raft.start();
    }

    @After
    public void tearDown() throws Exception {
        raft.stop();
        raft.close();
        injector.getInstance(LifecycleService.class).stop();
        injector.getInstance(LifecycleService.class).close();
    }

    // init

    @Test
    public void testBootstrapWithOneMember() throws Exception {
        start();
        electionTimeout();
        execute();
        Assert.assertEquals(Leader, raft.currentState());
    }

    @Test
    public void testInitStashClientMessage() throws Exception {
        appendClusterConf();
        start();
        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        execute();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
    }

    // follower

    @Test
    public void testStartAsFollower() throws Exception {
        appendClusterConf();
        start();
        execute();
        expectFollower();
    }

    @Test
    public void testFollowerStashClientMessage() throws Exception {
        appendClusterConf();
        start();
        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        execute();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
    }

    @Test
    public void testFollowerElectionTimeout() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
    }

    @Test
    public void testFollowerRejectVoteIfLastLogTermIsOld() throws Exception {
        appendClusterConf();
        start();
        execute();
        requestVote(1, node2, 0, 0);
        execute();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerRejectVoteIfLastLogIndexIsOld() throws Exception {
        appendClusterConf();
        start();
        execute();
        requestVote(1, node2, 1, 0);
        execute();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerRejectVoteIfAlreadyVoted() throws Exception {
        appendClusterConf();
        start();
        execute();
        requestVote(1, node2, 1, 1);
        requestVote(1, node3, 1, 1);
        execute();
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testFollowerRejectVoteIfTermOld() throws Exception {
        appendClusterConf();
        start();
        execute();
        requestVote(0, node2, 1, 1);
        execute();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }


    @Test
    public void testFollowerAcceptVoteIfTermIsNewer() throws Exception {
        appendClusterConf();
        start();
        execute();
        requestVote(2, node2, 1, 1);
        execute();
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
        expectTerm(2);
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndTermNotMatch() throws Exception {
        appendClusterConf();
        start();
        execute();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, new Term(1), 1, node2);
        raft.receive(appendEntries(node2, 1, 1, 0, 0, logEntry));
        execute();
        Assert.assertFalse(raft.currentLog().entries().contains(logEntry));
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndIndexNotMatch() throws Exception {
        appendClusterConf();
        start();
        execute();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, new Term(1), 1, node2);
        raft.receive(appendEntries(node2, 1, 0, 1, 0, logEntry));
        execute();
        Assert.assertFalse(raft.currentLog().entries().contains(logEntry));
    }

    @Test
    public void testFollowerAppendEntriesIfLogEmpty() throws Exception {
        appendClusterConf();
        start();
        execute();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, new Term(1), 2, node2);
        raft.receive(appendEntries(node2, 1, 1, 1, 0, logEntry));
        execute();
        Assert.assertTrue(raft.currentLog().entries().contains(logEntry));
    }

    // candidate

    @Test
    public void testCandidateRejectVoteIfLastLogTermIsOld() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        requestVote(1, node2, 0, 0);
        execute();
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfTermEqual() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        requestVote(2, node2, 1, 1);
        execute();
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfLastLogIndexIsOld() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        requestVote(3, node2, 1, 0);
        execute();
        expectFollower();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testCandidateRejectVoteIfAlreadyVoted() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        requestVote(3, node2, 1, 1);
        execute();
        expectFollower();
        requestVote(3, node3, 1, 1);
        execute();
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfTermOld() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        requestVote(0, node2, 1, 1);
        execute();
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateVoteIfTermIsNewer() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        requestVote(3, node2, 1, 1);
        execute();
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
        expectTerm(3);
    }

    @Test
    public void testCandidateVoteRejectIfTermOld() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        voteCandidate(node2, 1);
        execute();
        expectCandidate();
    }

    @Test
    public void testCandidateVoteRejectIfTermNewer() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        voteCandidate(node2, 3);
        execute();
        expectFollower();
        expectTerm(3);
    }

    @Test
    public void testCandidateVotedHasNotMajority() throws Exception {
        appendBigClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        voteCandidate(node2, 2);
        execute();
        expectCandidate();
        Assert.assertFalse(raft.currentMeta().hasMajority());
    }

    @Test
    public void testCandidateVotedHasMajority() throws Exception {
        appendBigClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        execute();
        expectLeader();
        Assert.assertFalse(raft.currentMeta().hasMajority());
    }

    @Test
    public void testCandidateDeclineCandidateWithTermNewer() throws Exception {
        appendBigClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        raft.receive(new DeclineCandidate(node2, new Term(3)));
        execute();
        expectFollower();
        expectTerm(3);
    }

    @Test
    public void testCandidateDeclineCandidateWithEqualTerm() throws Exception {
        appendBigClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        raft.receive(new DeclineCandidate(node2, new Term(2)));
        execute();
        expectCandidate();
    }

    @Test
    public void testCandidateDeclineCandidateWithTermOld() throws Exception {
        appendBigClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        raft.receive(new DeclineCandidate(node2, new Term(1)));
        execute();
        expectCandidate();
    }

    @Test
    public void testCandidateElectionTimeout() throws Exception {
        appendBigClusterConf();
        start();
        execute();
        expectFollower();
        expectTerm(1);
        electionTimeout();
        expectCandidate();
        expectTerm(2);
        electionTimeout();
        expectCandidate();
        expectTerm(3);
    }

    @Test
    public void testCandidateStashClientMessage() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        execute();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
    }

    @Test
    public void testCandidateBecameLeaderOnVoteMajority() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        voteCandidate(node2, 2);
        execute();
        expectLeader();
    }

    @Test
    public void testCandidateIgnoreAppendEntriesIfTermIsOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.receive(appendEntries(node2, 0, 1, 0, 0));
        execute();
        expectTerm(2);
        expectCandidate();
    }

    @Test
    public void testCandidateAppendEntriesIfTermIsEqual() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        LogEntry entry = new LogEntry(Noop.INSTANCE, new Term(1), 2, node2);
        raft.receive(appendEntries(node2, 2, 1, 1, 0, entry));
        execute();
        execute();
        expectFollower();
        expectTerm(2);
        Assert.assertTrue(raft.currentLog().entries().contains(entry));
    }

    // leader

    private void becameLeader() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        expectCandidate();
        verify(transportChannel2).message(new RequestVote(new Term(2), node1, new Term(1), 1));
        verify(transportChannel3).message(new RequestVote(new Term(2), node1, new Term(1), 1));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        execute();
        expectLeader();
    }

    @Test
    public void testLeaderIgnoreElectionMessage() throws Exception {
        becameLeader();
        expectLeader();
    }

    @Test
    public void testLeaderAppendStableClusterConfigurationOnElection() throws Exception {
        becameLeader();
        Assert.assertEquals(
            ImmutableList.of(stable(1, 1, node1, node2, node3), noop(2, 2, node1)),
            raft.currentLog().entries()
        );
    }

    @Test
    public void testLeaderSendHeartbeatOnElection() throws Exception {
        becameLeader();
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 1, noop(2, 2, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 1, noop(2, 2, node1)));
    }

    @Test
    public void testLeaderSendHeartbeatOnSendHeartbeat() throws Exception {
        becameLeader();
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 1, noop(2, 2, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 1, noop(2, 2, node1)));
        raft.receive(new AppendSuccessful(node2, new Term(2), 2));
        raft.receive(new AppendSuccessful(node3, new Term(2), 2));
        execute();

        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        execute();
        raft.receive(SendHeartbeat.INSTANCE);
        execute();

        verify(transportChannel2).message(appendEntries(node1, 2, 2, 2, 2, noop(2, 3, node2)));
        verify(transportChannel3).message(appendEntries(node1, 2, 2, 2, 2, noop(2, 3, node2)));
    }

    @Test
    public void testLeaderRejectAppendEntriesIfTermIsLower() throws Exception {
        becameLeader();
        raft.receive(appendEntries(node2, 1, 0, 0, 0));
        execute();
        expectLeader();
        expectTerm(2);
    }

    @Test
    public void testLeaderRejectAppendEntriesIfTermIsEqual() throws Exception {
        becameLeader();
        raft.receive(appendEntries(node2, 2, 0, 0, 0));
        execute();
        expectLeader();
        expectTerm(2);
    }

    @Test
    public void testLeaderStepDownOnAppendEntriesIfTermIsNewer() throws Exception {
        becameLeader();
        raft.receive(appendEntries(node2, 3, 0, 0, 0));
        execute();
        expectFollower();
        expectTerm(3);
    }

    @Test
    public void testLeaderStepDownOnAppendRejectedIfTermIsNewer() throws Exception {
        becameLeader();
        raft.receive(new AppendRejected(node2, new Term(3)));
        execute();
        expectFollower();
        expectTerm(3);
    }

    @Test
    public void testLeaderIgnoreAppendRejectedIfTermIsOld() throws Exception {
        becameLeader();
        raft.receive(new AppendRejected(node2, new Term(0)));
        execute();
        expectLeader();
        expectTerm(2);
    }

    @Test
    public void testLeaderSendPreviousMessageOnAppendRejectedIfTermMatches() throws Exception {
        appendClusterConf();
        log = log.append(noop(1, 2, node1)).append(noop(1, 3, node1));
        start();
        execute();
        electionTimeout();
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        execute();

        verify(transportChannel2).message(appendEntries(node1, 2, 1, 3, 1, noop(2, 4, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 3, 1, noop(2, 4, node1)));

        raft.receive(new AppendRejected(node2, new Term(2)));
        raft.receive(new AppendRejected(node3, new Term(2)));
        execute();

        // entry 4 does not included because different term
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 2, 1, noop(1, 3, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 2, 1, noop(1, 3, node1)));

        raft.receive(new AppendRejected(node2, new Term(2)));
        raft.receive(new AppendRejected(node3, new Term(2)));
        execute();

        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 1, noop(1, 2, node1), noop(1, 3, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 1, noop(1, 2, node1), noop(1, 3, node1)));
    }

    @Test
    public void testLeaderStepDownOnAppendSuccessfulIfTermIsNewer() throws Exception {
        becameLeader();
        raft.receive(new AppendSuccessful(node2, new Term(3), 2));
        execute();
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(1, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnAppendSuccessfulIfTermIsOld() throws Exception {
        becameLeader();
        raft.receive(new AppendSuccessful(node2, new Term(1), 2));
        raft.receive(new AppendSuccessful(node3, new Term(1), 2));
        execute();
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(1, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderCommitOnAppendSuccessfulIfTermMatches() throws Exception {
        becameLeader();
        raft.receive(new AppendSuccessful(node2, new Term(2), 2));
        raft.receive(new AppendSuccessful(node3, new Term(2), 2));
        execute();
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(2, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderRejectInstallSnapshotIfTermIsOld() throws Exception {
        becameLeader();
        StableClusterConfiguration conf = new StableClusterConfiguration(node1, node2, node3);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(new Term(1), 1, conf);
        raft.receive(new InstallSnapshot(node2, new Term(1), new RaftSnapshot(metadata, Noop.INSTANCE)));
        execute();
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(1, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderRejectInstallSnapshotIfTermIsEqual() throws Exception {
        becameLeader();
        StableClusterConfiguration conf = new StableClusterConfiguration(node1, node2, node3);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(new Term(1), 1, conf);
        raft.receive(new InstallSnapshot(node2, new Term(2), new RaftSnapshot(metadata, Noop.INSTANCE)));
        execute();
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(1, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotIfTermIsNewer() throws Exception {
        becameLeader();
        StableClusterConfiguration conf = new StableClusterConfiguration(node1, node2, node3);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(new Term(1), 1, conf);
        raft.receive(new InstallSnapshot(node2, new Term(3), new RaftSnapshot(metadata, Noop.INSTANCE)));
        execute();
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(1, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotSuccessfulIfTermIsNewer() throws Exception {
        becameLeader();
        raft.receive(new InstallSnapshotSuccessful(node2, new Term(3), 1));
        execute();
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(1, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderIgnoreInstallSnapshotSuccessfulIfTermIsOld() throws Exception {
        becameLeader();
        raft.receive(new InstallSnapshotSuccessful(node2, new Term(1), 1));
        execute();
        expectLeader();
        expectTerm(2);
    }

    @Test
    public void testLeaderHandleInstallSnapshotSuccessfulIfTermMatches() throws Exception {
        becameLeader();
        raft.receive(new InstallSnapshotSuccessful(node2, new Term(2), 2));
        raft.receive(new InstallSnapshotSuccessful(node3, new Term(2), 2));
        execute();
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(2, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotRejectedIfTermIsNew() throws Exception {
        becameLeader();
        raft.receive(new InstallSnapshotRejected(node2, new Term(3)));
        execute();
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(1, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderIgnoreInstallSnapshotRejectedIfTermIdOld() throws Exception {
        becameLeader();
        raft.receive(new InstallSnapshotRejected(node2, new Term(0)));
        execute();
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(1, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderHandleInstallSnapshotRejectedIfTermMatches() throws Exception {
        appendClusterConf();
        log = log.append(noop(1, 2, node1)).append(noop(1, 3, node1)).commit(3);
        start();
        execute();
        electionTimeout();
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        execute();
        raft.receive(new InstallSnapshotRejected(node2, new Term(2)));
        execute();
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 3, 3, noop(2, 4, node1)));
    }

    @Test
    public void testLeaderUnstashClientMessages() throws Exception {
        appendClusterConf();
        start();
        execute();
        expectFollower();
        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        electionTimeout();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        execute();
        Assert.assertEquals(ImmutableList.of(), raft.currentStashed());
    }

    // joint consensus

    @Test
    public void testJointNewNode() throws Exception {
        becameLeader();
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 1, noop(2, 2, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 1, noop(2, 2, node1)));
        appendSuccessful(node2, 2, 2);
        appendSuccessful(node3, 2, 2);
        execute();

        joint(node4);
        ClusterConfiguration stable = new StableClusterConfiguration(node1, node2, node3, node4);
        LogEntry stableEntry = new LogEntry(stable, new Term(2), 3, node4);
        verify(transportChannel2).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportChannel3).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        appendSuccessful(node2, 2, 3);
        appendSuccessful(node3, 2, 3);
        Assert.assertEquals(stable, raft.currentMeta().getConfig());
    }

    // additional methods

    private void joint(DiscoveryNode node) {
        raft.receive(new AddServer(node));
        execute();
    }

    private void requestVote(long term, DiscoveryNode node, long lastLogTerm, long lastLogIndex) {
        raft.receive(new RequestVote(new Term(term), node, new Term(lastLogTerm), lastLogIndex));
    }

    private void voteCandidate(DiscoveryNode node, long term) {
        raft.receive(new VoteCandidate(node, new Term(term)));
    }

    private void execute() {
        context.execute();
    }

    private void expectTerm(long term) {
        Assert.assertEquals(new Term(term), raft.currentMeta().getCurrentTerm());
    }

    private void expectCandidate() {
        Assert.assertEquals(Candidate, raft.currentState());
    }

    private void expectFollower() {
        Assert.assertEquals(Follower, raft.currentState());
    }

    private void expectLeader() {
        Assert.assertEquals(Leader, raft.currentState());
    }

    private void electionTimeout() throws Exception {
        raft.receive(ElectionTimeout.INSTANCE);
        execute();
    }

    private void sendHeartbeat() throws Exception {
        raft.receive(SendHeartbeat.INSTANCE);
        execute();
    }

    private LogEntry noop(long term, long index, DiscoveryNode client) {
        return new LogEntry(Noop.INSTANCE, new Term(term), index, client);
    }

    @SuppressWarnings("SameParameterValue")
    private LogEntry stable(long term, long index, DiscoveryNode... nodes) {
        return new LogEntry(new StableClusterConfiguration(nodes), new Term(term), index, node1);
    }

    @SuppressWarnings("SameParameterValue")
    private void appendSuccessful(DiscoveryNode node, long term, long index) {
        raft.receive(new AppendSuccessful(node, new Term(term), index));
        execute();
    }

    private AppendEntries appendEntries(DiscoveryNode node, long term, long prevTerm, long prevIndex, long commit, LogEntry... logEntries) {
        return new AppendEntries(node, new Term(term), new Term(prevTerm), prevIndex, ImmutableList.copyOf(logEntries), commit);
    }

    // test dependencies

    private class TestRaftContext implements RaftContext {

        private LinkedList<Runnable> queue = new LinkedList<>();

        @Override
        public void execute(Runnable runnable) {
            queue.add(runnable);
        }

        public void execute() {
            while (!queue.isEmpty()) {
                queue.pop().run();
            }
        }

        @Override
        public ScheduledFuture schedule(Runnable runnable, long timeout, TimeUnit timeUnit) {
            return new TestScheduledFuture();
        }

        @Override
        public ScheduledFuture scheduleAtFixedRate(Runnable runnable, long delay, long timeout, TimeUnit timeUnit) {
            return new TestScheduledFuture();
        }
    }

    private class TestScheduledFuture<V> implements ScheduledFuture<V> {

        @Override
        public long getDelay(TimeUnit timeUnit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed delayed) {
            return 0;
        }

        @Override
        public boolean cancel(boolean b) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public V get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }

    private class TestRaftModule extends AbstractModule {
        @Override
        protected void configure() {
            Preconditions.checkNotNull(transportService);
            Preconditions.checkNotNull(clusterDiscovery);
            Preconditions.checkNotNull(resourceFSM);
            Preconditions.checkNotNull(context);
            bind(TransportService.class).toInstance(transportService);
            bind(ClusterDiscovery.class).toInstance(clusterDiscovery);
            bind(ResourceFSM.class).toInstance(resourceFSM);
            bind(RaftContext.class).toInstance(context);
            bind(ReplicatedLog.class).toProvider(FileReplicatedLogProvider.class);

            Multibinder<StreamableRegistry> streamableBinder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendEntries.class, AppendEntries::new, 200));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendRejected.class, AppendRejected::new, 201));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendSuccessful.class, AppendSuccessful::new, 202));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(ClientMessage.class, ClientMessage::new, 210));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(ElectionTimeout.class, ElectionTimeout::read, 220));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(BeginElection.class, BeginElection::read, 221));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RequestVote.class, RequestVote::new, 223));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(VoteCandidate.class, VoteCandidate::new, 225));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(DeclineCandidate.class, DeclineCandidate::new, 226));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(ElectedAsLeader.class, ElectedAsLeader::read, 222));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(SendHeartbeat.class, SendHeartbeat::read, 224));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(LogEntry.class, LogEntry::new, 230));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(Noop.class, Noop::read, 231));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftSnapshot.class, RaftSnapshot::new, 240));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftSnapshotMetadata.class, RaftSnapshotMetadata::new, 241));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(InitLogSnapshot.class, InitLogSnapshot::read, 260));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshot.class, InstallSnapshot::new, 261));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshotRejected.class, InstallSnapshotRejected::new, 262));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshotSuccessful.class, InstallSnapshotSuccessful::new, 263));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(JointConsensusClusterConfiguration.class, JointConsensusClusterConfiguration::new, 270));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(StableClusterConfiguration.class, StableClusterConfiguration::new, 271));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AddServer.class, AddServer::new, 280));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AddServerResponse.class, AddServerResponse::new, 281));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RemoveServer.class, RemoveServer::new, 282));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RemoveServerResponse.class, RemoveServerResponse::new, 283));
        }
    }
}
