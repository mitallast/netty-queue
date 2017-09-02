package org.mitallast.queue.raft;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import javaslang.collection.HashMap;
import javaslang.collection.HashSet;
import javaslang.collection.Vector;
import javaslang.control.Option;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.common.component.ComponentModule;
import org.mitallast.queue.common.component.LifecycleService;
import org.mitallast.queue.common.events.EventBus;
import org.mitallast.queue.common.file.FileModule;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.persistent.FilePersistentService;
import org.mitallast.queue.raft.persistent.PersistentService;
import org.mitallast.queue.raft.persistent.ReplicatedLog;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.raft.resource.ResourceRegistry;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mitallast.queue.raft.RaftState.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class RaftTest extends BaseTest {

    static {
        Codec.Companion.register(5555555, TestFSMMessage.class, TestFSMMessage.codec);
    }

    private Injector injector;

    @Mock
    private TransportService transportService;

    @Mock
    private TransportController transportController;

    @Mock
    private ClusterDiscovery clusterDiscovery;

    @Mock
    private ResourceRegistry registry;

    @Mock
    private EventBus eventBus;

    private TestRaftContext context;

    private Config config;

    private Raft raft;

    private PersistentService persistentService;
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
        when(clusterDiscovery.discoveryNodes()).thenReturn(HashSet.empty());
        when(registry.apply(0, TestFSMMessage.INSTANCE)).thenReturn(TestFSMMessage.INSTANCE);
        when(registry.apply(1, TestFSMMessage.INSTANCE)).thenReturn(TestFSMMessage.INSTANCE);
        when(registry.apply(2, TestFSMMessage.INSTANCE)).thenReturn(TestFSMMessage.INSTANCE);
        when(registry.apply(3, TestFSMMessage.INSTANCE)).thenReturn(TestFSMMessage.INSTANCE);

        context = new TestRaftContext();
        config = ConfigFactory.defaultReference();
        override("node.path", testFolder.getRoot().getAbsolutePath());
        override("crdt.enabled", "false");
        override("raft.enabled", "true");
        override("raft.bootstrap", "true");
        override("raft.snapshot-interval", "100");
        injector = Guice.createInjector(
            new ComponentModule(config),
            new FileModule(),
            new TestRaftModule()
        );
        config = injector.getInstance(Config.class);
        injector.getInstance(LifecycleService.class).start();
        persistentService = injector.getInstance(PersistentService.class);
        log = persistentService.openLog();
    }

    private void appendClusterSelf() throws Exception {
        log = log.append(new LogEntry(1, 1, 0, new StableClusterConfiguration(node1))).commit(1);
    }

    private void appendClusterConf() throws Exception {
        log = log.append(new LogEntry(1, 1, 0, new StableClusterConfiguration(node1, node2, node3))).commit(1);
    }

    private void appendBigClusterConf() throws Exception {
        log = log.append(new LogEntry(1, 1, 0, new StableClusterConfiguration(node1, node2, node3, node4, node5)))
            .commit(1);
    }

    private void override(String key, String value) {
        config = ConfigFactory.parseMap(HashMap.of(key, value).toJavaMap()).withFallback(config);
    }

    private void start() throws Exception {
        log.close();
        log = null;
        raft = new Raft(
            config,
            injector.getInstance(TransportService.class),
            injector.getInstance(TransportController.class),
            injector.getInstance(ClusterDiscovery.class),
            persistentService,
            registry,
            context,
            eventBus
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
        expectLeader();
    }

    @Test
    public void testJointCluster() throws Exception {
        override("raft.bootstrap", "false");
        when(clusterDiscovery.discoveryNodes()).thenReturn(HashSet.of(node1, node2, node3));
        start();
        expectFollower();
        Assert.assertEquals(HashSet.of(node1, node2, node3), clusterDiscovery.discoveryNodes());
        verify(transportService).send(node2, new AddServer(node1));
        verify(transportService).send(node3, new AddServer(node1));
    }

    // follower

    @Test
    public void testStartAsFollower() throws Exception {
        appendClusterConf();
        start();
        expectFollower();
    }

    @Test
    public void testFollowerStashClientMessage() throws Exception {
        appendClusterConf();
        start();
        raft.apply(new ClientMessage(Noop.INSTANCE, 0));
        Assert.assertEquals(Vector.of(new ClientMessage(Noop.INSTANCE, 0)), raft.currentStashed());
    }

    @Test
    public void testFollowerSendClientMessageToRecentLeader() throws Exception {
        appendClusterConf();
        start();
        expectFollower();
        expectTerm(1);
        raft.apply(appendEntries(node2, 1, 1, 1, 1));
        Assert.assertEquals(Option.some(node2), raft.recentLeader());

        raft.apply(new ClientMessage(Noop.INSTANCE, 0));
        Assert.assertEquals(Vector.empty(), raft.currentStashed());
        verify(transportService).send(node2, new ClientMessage(Noop.INSTANCE, 0));
    }

    @Test
    public void testFollowerElectionTimeout() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
    }

    @Test
    public void testFollowerDoesNotBeginElectionWithoutNodes() throws Exception {
        override("raft.bootstrap", "false");
        start();
        expectFollower();
        Assert.assertEquals(HashSet.empty(), clusterDiscovery.discoveryNodes());
        electionTimeout();
        expectFollower();
    }

    @Test
    public void testFollowerRejectVoteIfLastLogTermIsOld() throws Exception {
        appendClusterConf();
        start();
        requestVote(1, node2, 0, 0);
        Assert.assertFalse(raft.currentMeta().getVotedFor().isDefined());
    }

    @Test
    public void testFollowerRejectVoteIfLastLogIndexIsOld() throws Exception {
        appendClusterConf();
        start();
        requestVote(1, node2, 1, 0);
        Assert.assertFalse(raft.currentMeta().getVotedFor().isDefined());
    }

    @Test
    public void testFollowerRejectVoteIfAlreadyVoted() throws Exception {
        override("raft.bootstrap", "false");
        start();
        requestVote(1, node2, 1, 1);
        requestVote(1, node3, 1, 1);
        Assert.assertEquals(Option.some(node2), raft.currentMeta().getVotedFor());
    }

    @Test
    public void testFollowerRejectVoteIfTermOld() throws Exception {
        appendClusterConf();
        start();
        requestVote(0, node2, 1, 1);
        Assert.assertFalse(raft.currentMeta().getVotedFor().isDefined());
    }

    @Test
    public void testFollowerAcceptVoteIfTermIsNewer() throws Exception {
        override("raft.bootstrap", "false");
        start();
        requestVote(2, node2, 1, 1);
        Assert.assertEquals(Option.some(node2), raft.currentMeta().getVotedFor());
        expectTerm(2);
    }

    @Test
    public void testFollowerRejectAppendEntriesIfTermIsOld() throws Exception {
        override("raft.bootstrap", "false");
        start();
        LogEntry logEntry = new LogEntry(1, 1, 0, Noop.INSTANCE);
        raft.apply(appendEntries(node2, 1, 1, 0, 0, logEntry));
        Assert.assertFalse(raft.replicatedLog().contains(logEntry));
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndTermNotMatch() throws Exception {
        override("raft.bootstrap", "false");
        start();
        LogEntry logEntry = new LogEntry(1, 1, 0, Noop.INSTANCE);
        raft.apply(appendEntries(node2, 0, 1, 0, 0, logEntry));
        Assert.assertFalse(raft.replicatedLog().contains(logEntry));
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndIndexNotMatch() throws Exception {
        override("raft.bootstrap", "false");
        start();
        LogEntry logEntry = new LogEntry(1, 1, 0, Noop.INSTANCE);
        raft.apply(appendEntries(node2, 1, 0, 1, 0, logEntry));
        Assert.assertFalse(raft.replicatedLog().contains(logEntry));
    }

    @Test
    public void testFollowerAppendEntriesIfLogEmpty() throws Exception {
        override("raft.bootstrap", "false");
        start();
        LogEntry logEntry = new LogEntry(1, 1, 0, Noop.INSTANCE);
        raft.apply(appendEntries(node2, 1, 0, 0, 0, logEntry));
        Assert.assertTrue(raft.replicatedLog().contains(logEntry));
    }

    @Test
    public void testFollowerBecameLeaderOnSelfElection() throws Exception {
        appendClusterSelf();
        start();
        electionTimeout();
        expectLeader();
    }

    @Test
    public void testFollowerIgnoreAppendSuccessful() throws Exception {
        appendClusterSelf();
        start();
        appendSuccessful(node2, 2, 2);
        expectFollower();
    }

    @Test
    public void testFollowerIgnoreAppendRejected() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(new AppendRejected(node2, 1, 1));
        expectFollower();
    }

    @Test
    public void testFollowerIgnoreDeclineCandidate() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(new DeclineCandidate(node2, 1));
        expectFollower();
        expectTerm(1);
    }

    @Test
    public void testFollowerIgnoreInstallSnapshotSuccessful() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(new InstallSnapshotSuccessful(node2, 2, 1));
        expectFollower();
        expectTerm(1);
    }

    @Test
    public void testFollowerIgnoreInstallInstallSnapshotRejected() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(new InstallSnapshotRejected(node2, 2));
        expectFollower();
        expectTerm(1);
    }

    @Test
    public void testFollowerRejectInstallSnapshotIfTermIsOld() throws Exception {
        appendClusterSelf();
        start();
        ClusterConfiguration conf = new StableClusterConfiguration(node1);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        RaftSnapshot snapshot = new RaftSnapshot(metadata, Vector.empty());
        raft.apply(new InstallSnapshot(node4, 0, snapshot));
        expectFollower();
        expectTerm(1);
        verify(transportService).send(node4, new InstallSnapshotRejected(node1, 1));
    }

    @Test
    public void testFollowerInstallSnapshotIfTermIsNewer() throws Exception {
        appendClusterSelf();
        start();
        ClusterConfiguration conf = new StableClusterConfiguration(node1);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        RaftSnapshot snapshot = new RaftSnapshot(metadata, Vector.empty());
        raft.apply(new InstallSnapshot(node4, 2, snapshot));
        expectFollower();
        expectTerm(2);
        verify(transportService).send(node4, new InstallSnapshotSuccessful(node1, 2, 1));
    }

    @Test
    public void testFollowerInstallSnapshotIfTermIsEqual() throws Exception {
        appendClusterSelf();
        start();
        ClusterConfiguration conf = new StableClusterConfiguration(node1);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        RaftSnapshot snapshot = new RaftSnapshot(metadata, Vector.empty());
        raft.apply(new InstallSnapshot(node4, 1, snapshot));
        expectFollower();
        expectTerm(1);
        verify(transportService).send(node4, new InstallSnapshotSuccessful(node1, 1, 1));
    }

    @Test
    public void testFollowerRejectAddServer() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(new AddServer(node4));
        verify(transportService).send(node4, new AddServerResponse(AddServerResponse.Status.NOT_LEADER, Option.none()));
    }

    @Test
    public void testFollowerRejectRemoveServer() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(new RemoveServer(node4));
        verify(transportService).send(node4, new RemoveServerResponse(RemoveServerResponse.Status.NOT_LEADER, Option
            .none()));
    }

    // candidate

    @Test
    public void testCandidateRejectAddServer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(new AddServer(node4));
        verify(transportService).send(node4, new AddServerResponse(AddServerResponse.Status.NOT_LEADER, Option.none()));
    }

    @Test
    public void testCandidateRejectRemoveServer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        verify(transportService).send(node2, new RequestVote(2, node1, 1, 1));
        verify(transportService).send(node3, new RequestVote(2, node1, 1, 1));

        raft.apply(new RemoveServer(node3));
        verify(transportService).send(node3, new RemoveServerResponse(RemoveServerResponse.Status.NOT_LEADER, Option
            .none()));
    }

    @Test
    public void testCandidateRejectVoteIfLastLogTermIsOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        requestVote(1, node2, 0, 0);
        Assert.assertEquals(Option.some(node1), raft.currentMeta().getVotedFor());
    }

    @Test
    public void testCandidateRejectVoteIfTermEqual() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        requestVote(2, node2, 1, 1);
        Assert.assertEquals(Option.some(node1), raft.currentMeta().getVotedFor());
    }

    @Test
    public void testCandidateRejectVoteIfLastLogIndexIsOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        requestVote(3, node2, 1, 0);
        expectFollower();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isDefined());
    }

    @Test
    public void testCandidateRejectVoteIfAlreadyVoted() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        requestVote(3, node2, 1, 1);
        expectFollower();
        requestVote(3, node3, 1, 1);
        Assert.assertEquals(Option.some(node2), raft.currentMeta().getVotedFor());
    }

    @Test
    public void testCandidateRejectVoteIfTermOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        requestVote(0, node2, 1, 1);
        Assert.assertEquals(Option.some(node1), raft.currentMeta().getVotedFor());
    }

    @Test
    public void testCandidateVoteIfTermIsNewer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        requestVote(3, node2, 1, 1);
        Assert.assertEquals(Option.some(node2), raft.currentMeta().getVotedFor());
        expectTerm(3);
    }

    @Test
    public void testCandidateVoteRejectIfTermOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        voteCandidate(node2, 1);
        expectCandidate();
    }

    @Test
    public void testCandidateVoteRejectIfTermNewer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        voteCandidate(node2, 3);
        expectFollower();
        expectTerm(3);
    }

    @Test
    public void testCandidateVotedHasNotMajority() throws Exception {
        appendBigClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        voteCandidate(node2, 2);
        expectCandidate();
        Assert.assertFalse(raft.currentMeta().hasMajority());
    }

    @Test
    public void testCandidateVotedHasMajority() throws Exception {
        appendBigClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        expectLeader();
        Assert.assertTrue(raft.currentMeta().hasMajority());
    }

    @Test
    public void testCandidateDeclineCandidateWithTermNewer() throws Exception {
        appendBigClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(new DeclineCandidate(node2, 3));
        expectFollower();
        expectTerm(3);
    }

    @Test
    public void testCandidateDeclineCandidateWithEqualTerm() throws Exception {
        appendBigClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(new DeclineCandidate(node2, 2));
        expectCandidate();
    }

    @Test
    public void testCandidateDeclineCandidateWithTermOld() throws Exception {
        appendBigClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(new DeclineCandidate(node2, 1));
        expectCandidate();
    }

    @Test
    public void testCandidateElectionTimeout() throws Exception {
        appendBigClusterConf();
        start();
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
        electionTimeout();
        raft.apply(new ClientMessage(Noop.INSTANCE, 0));
        Assert.assertEquals(Vector.of(new ClientMessage(Noop.INSTANCE, 0)), raft.currentStashed());
    }

    @Test
    public void testCandidateBecameLeaderOnVoteMajority() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        voteCandidate(node2, 2);
        expectLeader();
    }

    @Test
    public void testCandidateIgnoreAppendEntriesIfTermIsOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(appendEntries(node2, 0, 1, 0, 0));
        expectTerm(2);
        expectCandidate();
    }

    @Test
    public void testCandidateAppendEntriesIfTermIsEqual() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        LogEntry entry = new LogEntry(1, 2, 0, Noop.INSTANCE);
        raft.apply(appendEntries(node2, 2, 1, 1, 0, entry));
        expectFollower();
        expectTerm(2);
        Assert.assertTrue(raft.replicatedLog().contains(entry));
    }

    @Test
    public void testCandidateInstallSnapshotIfTermIsEqual() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        expectTerm(2);
        ClusterConfiguration conf = new StableClusterConfiguration(node1);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        RaftSnapshot snapshot = new RaftSnapshot(metadata, Vector.empty());
        raft.apply(new InstallSnapshot(node4, 2, snapshot));
        expectFollower();
        expectTerm(2);
        verify(transportService).send(node4, new InstallSnapshotSuccessful(node1, 2, 1));
    }

    @Test
    public void testCandidateInstallSnapshotIfTermIsNewer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        expectTerm(2);
        ClusterConfiguration conf = new StableClusterConfiguration(node1);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        RaftSnapshot snapshot = new RaftSnapshot(metadata, Vector.empty());
        raft.apply(new InstallSnapshot(node4, 3, snapshot));
        expectFollower();
        expectTerm(3);
        verify(transportService).send(node4, new InstallSnapshotSuccessful(node1, 3, 1));
    }

    @Test
    public void testCandidateRejectInstallSnapshotIfTermIsOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        expectTerm(2);
        ClusterConfiguration conf = new StableClusterConfiguration(node1);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        RaftSnapshot snapshot = new RaftSnapshot(metadata, Vector.empty());
        raft.apply(new InstallSnapshot(node4, 1, snapshot));
        expectCandidate();
        expectTerm(2);
        verify(transportService).send(node4, new InstallSnapshotRejected(node1, 2));
    }

    @Test
    public void testCandidateIgnoreAddServerResponse() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        expectTerm(2);
        raft.apply(new AddServerResponse(AddServerResponse.Status.OK, Option.none()));
        expectCandidate();
        expectTerm(2);
    }

    @Test
    public void testCandidateIgnoreRemoveServerResponse() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        expectTerm(2);
        raft.apply(new RemoveServerResponse(RemoveServerResponse.Status.OK, Option.none()));
        expectCandidate();
        expectTerm(2);
    }

    // leader

    private void becameLeader() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        verify(transportService).send(node2, new RequestVote(2, node1, 1, 1));
        verify(transportService).send(node3, new RequestVote(2, node1, 1, 1));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        expectLeader();
        expectTerm(2);
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
            Vector.of(stable(1, 1, node1, node2, node3), noop(2, 2)),
            raft.replicatedLog().entries()
        );
    }

    @Test
    public void testLeaderSendHeartbeatOnElection() throws Exception {
        becameLeader();
        verify(transportService).send(node2, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        verify(transportService).send(node3, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
    }

    @Test
    public void testLeaderSendHeartbeatOnSendHeartbeat() throws Exception {
        becameLeader();
        verify(transportService).send(node2, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        verify(transportService).send(node3, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        raft.apply(new AppendSuccessful(node2, 2, 2));
        raft.apply(new AppendSuccessful(node3, 2, 2));

        raft.apply(new ClientMessage(Noop.INSTANCE, 0));
        context.runTimer(RaftContext.SEND_HEARTBEAT);

        verify(transportService).send(node2, appendEntries(node1, 2, 2, 2, 2, noop(2, 3)));
        verify(transportService).send(node3, appendEntries(node1, 2, 2, 2, 2, noop(2, 3)));
    }

    @Test
    public void testLeaderRejectAppendEntriesIfTermIsLower() throws Exception {
        becameLeader();
        raft.apply(appendEntries(node2, 1, 0, 0, 0));
        expectLeader();
        expectTerm(2);
    }

    @Test
    public void testLeaderRejectAppendEntriesIfTermIsEqual() throws Exception {
        becameLeader();
        raft.apply(appendEntries(node2, 2, 0, 0, 0));
        expectLeader();
        expectTerm(2);
    }

    @Test
    public void testLeaderStepDownOnAppendEntriesIfTermIsNewer() throws Exception {
        becameLeader();
        raft.apply(appendEntries(node2, 3, 0, 0, 0));
        expectFollower();
        expectTerm(3);
    }

    @Test
    public void testLeaderStepDownOnAppendRejectedIfTermIsNewer() throws Exception {
        becameLeader();
        raft.apply(new AppendRejected(node2, 3, 1));
        expectFollower();
        expectTerm(3);
    }

    @Test
    public void testLeaderIgnoreAppendRejectedIfTermIsOld() throws Exception {
        becameLeader();
        raft.apply(new AppendRejected(node2, 0, 1));
        expectLeader();
        expectTerm(2);
    }

    @Test
    public void testLeaderSendPreviousMessageOnAppendRejectedIfTermMatches() throws Exception {
        appendClusterConf();
        log = log.append(noop(1, 2)).append(noop(1, 3));
        start();
        electionTimeout();
        verify(transportService).send(eq(node2), any(RequestVote.class));
        verify(transportService).send(eq(node3), any(RequestVote.class));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);

        verify(transportService).send(node2, appendEntries(node1, 2, 1, 3, 0, noop(2, 4)));
        verify(transportService).send(node3, appendEntries(node1, 2, 1, 3, 0, noop(2, 4)));

        raft.apply(new AppendRejected(node2, 2, 4));
        raft.apply(new AppendRejected(node3, 2, 4));

        // entry 4 does not included because different term
        verify(transportService).send(node2, appendEntries(node1, 2, 1, 2, 0, noop(1, 3)));
        verify(transportService).send(node3, appendEntries(node1, 2, 1, 2, 0, noop(1, 3)));

        raft.apply(new AppendRejected(node2, 2, 4));
        raft.apply(new AppendRejected(node3, 2, 4));

        verify(transportService).send(node2, appendEntries(node1, 2, 1, 1, 0, noop(1, 2), noop(1, 3)));
        verify(transportService).send(node3, appendEntries(node1, 2, 1, 1, 0, noop(1, 2), noop(1, 3)));
    }

    @Test
    public void testLeaderStepDownOnAppendSuccessfulIfTermIsNewer() throws Exception {
        becameLeader();
        raft.apply(new AppendSuccessful(node2, 3, 2));
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnAppendSuccessfulIfTermIsOld() throws Exception {
        becameLeader();
        raft.apply(new AppendSuccessful(node2, 1, 2));
        raft.apply(new AppendSuccessful(node3, 1, 2));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderCommitOnAppendSuccessfulIfTermMatches() throws Exception {
        becameLeader();
        raft.apply(new AppendSuccessful(node2, 2, 2));
        raft.apply(new AppendSuccessful(node3, 2, 2));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(2, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderRejectInstallSnapshotIfTermIsOld() throws Exception {
        becameLeader();
        StableClusterConfiguration conf = new StableClusterConfiguration(node1, node2, node3);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        raft.apply(new InstallSnapshot(node2, 1, new RaftSnapshot(metadata, Vector.empty())));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderRejectInstallSnapshotIfTermIsEqual() throws Exception {
        becameLeader();
        StableClusterConfiguration conf = new StableClusterConfiguration(node1, node2, node3);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        raft.apply(new InstallSnapshot(node2, 2, new RaftSnapshot(metadata, Vector.empty())));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotIfTermIsNewer() throws Exception {
        becameLeader();
        StableClusterConfiguration conf = new StableClusterConfiguration(node1, node2, node3);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        raft.apply(new InstallSnapshot(node2, 3, new RaftSnapshot(metadata, Vector.empty())));
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotSuccessfulIfTermIsNewer() throws Exception {
        becameLeader();
        raft.apply(new InstallSnapshotSuccessful(node2, 3, 1));
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderIgnoreInstallSnapshotSuccessfulIfTermIsOld() throws Exception {
        becameLeader();
        raft.apply(new InstallSnapshotSuccessful(node2, 1, 1));
        expectLeader();
        expectTerm(2);
    }

    @Test
    public void testLeaderHandleInstallSnapshotSuccessfulIfTermMatches() throws Exception {
        becameLeader();
        raft.apply(new InstallSnapshotSuccessful(node2, 2, 2));
        raft.apply(new InstallSnapshotSuccessful(node3, 2, 2));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(2, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotRejectedIfTermIsNew() throws Exception {
        becameLeader();
        raft.apply(new InstallSnapshotRejected(node2, 3));
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderIgnoreInstallSnapshotRejectedIfTermIdOld() throws Exception {
        becameLeader();
        raft.apply(new InstallSnapshotRejected(node2, 0));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderHandleInstallSnapshotRejectedIfTermMatches() throws Exception {
        appendClusterConf();
        log = log.append(noop(1, 2)).append(noop(1, 3));
        start();
        electionTimeout();
        verify(transportService).send(eq(node2), any(RequestVote.class));
        verify(transportService).send(eq(node3), any(RequestVote.class));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        raft.apply(new InstallSnapshotRejected(node2, 2));
        verify(transportService).send(node2, appendEntries(node1, 2, 1, 3, 0, noop(2, 4)));
    }

    @Test
    public void testLeaderUnstashClientMessages() throws Exception {
        appendClusterConf();
        start();
        expectFollower();
        raft.apply(new ClientMessage(Noop.INSTANCE, 0));
        electionTimeout();
        Assert.assertEquals(Vector.of(new ClientMessage(Noop.INSTANCE, 0)), raft.currentStashed());
        verify(transportService).send(eq(node2), any(RequestVote.class));
        verify(transportService).send(eq(node3), any(RequestVote.class));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        expectLeader();
        Assert.assertEquals(Vector.empty(), raft.currentStashed());
    }

    @Test
    public void testLeaderRejectVoteIfTermIsEqual() throws Exception {
        becameLeader();
        requestVote(2, node2, 2, 2);
        expectLeader();
        expectTerm(2);
        verify(transportService).send(node2, new DeclineCandidate(node1, 2));
    }

    @Test
    public void testLeaderVoteCandidateIfTermIsNewer() throws Exception {
        becameLeader();
        requestVote(3, node2, 2, 2);
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(Option.some(node2), raft.currentMeta().getVotedFor());
        verify(transportService).send(node2, new VoteCandidate(node1, 3));
    }

    @Test
    public void testLeaderCreateSnapshot() throws Exception {
        // snapshot
        RaftSnapshotMetadata meta = new RaftSnapshotMetadata(2, 100, new StableClusterConfiguration(node1, node2,
            node3));
        RaftSnapshot snapshot = new RaftSnapshot(meta, Vector.empty());
        when(registry.prepareSnapshot(meta)).thenReturn(snapshot);

        becameLeader();
        for (int i = 0; i < 100; i++) {
            raft.apply(new ClientMessage(Noop.INSTANCE, 0));
        }

        verify(transportService).send(node2, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        verify(transportService).send(node3, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        // commit index 1
        appendSuccessful(node2, 2, 100);
        appendSuccessful(node3, 2, 100);

        ReplicatedLog log = raft.replicatedLog();
        logger.info("log: {}", log);
        Assert.assertTrue(log.hasSnapshot());
        Assert.assertEquals(meta, log.snapshot().getMeta());

        // send install snapshot
        raft.apply(new AppendRejected(node3, 2, 1));
        verify(transportService).send(node3, new InstallSnapshot(node1, 2, snapshot));
    }

    // joint consensus

    @Test
    public void testAddServer() throws Exception {
        becameLeader();
        verify(transportService).send(node2, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        verify(transportService).send(node3, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        appendSuccessful(node2, 2, 2);
        appendSuccessful(node3, 2, 2);

        raft.apply(new AddServer(node4));
        ClusterConfiguration stable = new StableClusterConfiguration(node1, node2, node3, node4);
        LogEntry stableEntry = new LogEntry(2, 3, 0, stable);
        verify(transportService).send(node2, appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportService).send(node3, appendEntries(node1, 2, 2, 2, 2, stableEntry));
        appendSuccessful(node2, 2, 3);
        appendSuccessful(node3, 2, 3);
        Assert.assertEquals(stable, raft.currentMeta().getConfig());
    }

    @Test
    public void testAddServerInTransitioningState() throws Exception {
        becameLeader();
        verify(transportService).send(node2, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        verify(transportService).send(node3, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        appendSuccessful(node2, 2, 2);
        appendSuccessful(node3, 2, 2);

        raft.apply(new AddServer(node4));
        ClusterConfiguration stable = new StableClusterConfiguration(node1, node2, node3, node4);
        LogEntry stableEntry = new LogEntry(2, 3, 0, stable);
        verify(transportService).send(node2, appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportService).send(node3, appendEntries(node1, 2, 2, 2, 2, stableEntry));
        expectLeader();
        Assert.assertTrue(raft.currentMeta().getConfig().isTransitioning());

        raft.apply(new AddServer(node5));
        verify(transportService).send(node5, new AddServerResponse(AddServerResponse.Status.TIMEOUT, Option.some
            (node1)));
    }

    @Test
    public void testRemoveServer() throws Exception {
        becameLeader();
        verify(transportService).send(node2, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        verify(transportService).send(node3, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        appendSuccessful(node2, 2, 2);
        appendSuccessful(node3, 2, 2);

        raft.apply(new RemoveServer(node3));
        ClusterConfiguration stable = new StableClusterConfiguration(node1, node2);
        LogEntry stableEntry = new LogEntry(2, 3, 0, stable);
        verify(transportService).send(node2, appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportService).send(node3, appendEntries(node1, 2, 2, 2, 2, stableEntry));
        appendSuccessful(node2, 2, 3);
        appendSuccessful(node3, 2, 3);
        Assert.assertEquals(stable, raft.currentMeta().getConfig());
    }

    @Test
    public void testRemoveServerInTransitioningState() throws Exception {
        becameLeader();
        verify(transportService).send(node2, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        verify(transportService).send(node3, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        appendSuccessful(node2, 2, 2);
        appendSuccessful(node3, 2, 2);

        raft.apply(new RemoveServer(node3));
        ClusterConfiguration stable = new StableClusterConfiguration(node1, node2);
        LogEntry stableEntry = new LogEntry(2, 3, 0, stable);
        verify(transportService).send(node2, appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportService).send(node3, appendEntries(node1, 2, 2, 2, 2, stableEntry));
        expectLeader();
        Assert.assertTrue(raft.currentMeta().getConfig().isTransitioning());

        raft.apply(new RemoveServer(node2));
        verify(transportService).send(node2, new RemoveServerResponse(RemoveServerResponse.Status.TIMEOUT, Option
            .some(node1)));
    }

    @Test
    public void testLeaderRemoveSelf() throws Exception {
        becameLeader();
        verify(transportService).send(node2, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        verify(transportService).send(node3, appendEntries(node1, 2, 1, 1, 0, noop(2, 2)));
        appendSuccessful(node2, 2, 2);
        appendSuccessful(node3, 2, 2);

        raft.apply(new RemoveServer(node1));
        ClusterConfiguration stable = new StableClusterConfiguration(node2, node3);
        LogEntry stableEntry = new LogEntry(2, 3, 0, stable);
        verify(transportService).send(node2, appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportService).send(node3, appendEntries(node1, 2, 2, 2, 2, stableEntry));
        appendSuccessful(node2, 2, 3);
        appendSuccessful(node3, 2, 3);
        expectFollower();
        Assert.assertEquals(stable, raft.currentMeta().getConfig());
    }

    @Test
    public void testFollowerHandleAddServerResponse() throws Exception {
        log = log.append(new LogEntry(1, 1, 0, Noop.INSTANCE)).commit(1);
        start();
        expectFollower();
        raft.apply(new AddServerResponse(AddServerResponse.Status.OK, Option.some(node2)));
        Assert.assertEquals(Option.some(node2), raft.recentLeader());
    }

    @Test
    public void testFollowerHandleRemoveServerResponse() throws Exception {
        log = log.append(new LogEntry(1, 1, 0, Noop.INSTANCE)).commit(1);
        start();
        expectFollower();
        raft.apply(new RemoveServerResponse(RemoveServerResponse.Status.OK, Option.some(node2)));
        Assert.assertEquals(Option.some(node2), raft.recentLeader());
    }

    // additional methods

    private void requestVote(long term, DiscoveryNode node, long lastLogTerm, long lastLogIndex) {
        raft.apply(new RequestVote(term, node, lastLogTerm, lastLogIndex));
    }

    private void voteCandidate(DiscoveryNode node, long term) {
        raft.apply(new VoteCandidate(node, term));
    }

    private void expectTerm(long term) {
        Assert.assertEquals(term, raft.currentMeta().getCurrentTerm());
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
        context.runTimer(RaftContext.ELECTION_TIMEOUT);
    }

    private LogEntry noop(long term, long index) {
        return new LogEntry(term, index, 0, Noop.INSTANCE);
    }

    @SuppressWarnings("SameParameterValue")
    private LogEntry stable(long term, long index, DiscoveryNode... nodes) {
        return new LogEntry(term, index, 0, new StableClusterConfiguration(nodes));
    }

    @SuppressWarnings("SameParameterValue")
    private void appendSuccessful(DiscoveryNode node, long term, long index) {
        raft.apply(new AppendSuccessful(node, term, index));
    }

    private AppendEntries appendEntries(DiscoveryNode node, long term, long prevTerm, long prevIndex, long commit,
                                        LogEntry... logEntries) {
        return new AppendEntries(node, term, prevTerm, prevIndex, commit, Vector.of(logEntries));
    }

    // test dependencies

    private static class TestFSMMessage implements Message {
        public static final TestFSMMessage INSTANCE = new TestFSMMessage();
        public static final Codec<TestFSMMessage> codec = Codec.Companion.of(INSTANCE);

        private TestFSMMessage() {
        }
    }

    private class TestRaftContext implements RaftContext {

        private ConcurrentMap<String, Runnable> timers = new ConcurrentHashMap<>();

        @Override
        public void setTimer(String name, long delayMs, Runnable task) {
            timers.put(name, task);
        }

        @Override
        public void startTimer(String name, long delayMs, long periodMs, Runnable task) {
            timers.put(name, task);
        }

        @Override
        public void cancelTimer(String name) {
            timers.remove(name);
        }

        public void runTimer(String name) {
            Runnable runnable = timers.get(name);
            Assert.assertNotNull(runnable);
            runnable.run();
        }
    }

    private class TestRaftModule extends AbstractModule {
        @Override
        protected void configure() {
            Preconditions.checkNotNull(transportService);
            Preconditions.checkNotNull(transportController);
            Preconditions.checkNotNull(clusterDiscovery);
            Preconditions.checkNotNull(registry);
            Preconditions.checkNotNull(context);
            bind(TransportService.class).toInstance(transportService);
            bind(TransportController.class).toInstance(transportController);
            bind(ClusterDiscovery.class).toInstance(clusterDiscovery);
            bind(ResourceRegistry.class).toInstance(registry);
            bind(RaftContext.class).toInstance(context);
            bind(FilePersistentService.class).asEagerSingleton();
            bind(PersistentService.class).to(FilePersistentService.class);
        }
    }
}
