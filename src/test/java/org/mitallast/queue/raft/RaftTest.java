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
import org.mitallast.queue.Version;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.component.ComponentModule;
import org.mitallast.queue.common.component.LifecycleService;
import org.mitallast.queue.common.file.FileModule;
import org.mitallast.queue.common.stream.*;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.persistent.FilePersistentService;
import org.mitallast.queue.raft.persistent.PersistentService;
import org.mitallast.queue.raft.persistent.ReplicatedLog;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.raft.resource.ResourceRegistry;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Optional;
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
    private TransportController transportController;

    @Mock
    private ClusterDiscovery clusterDiscovery;

    @Mock
    private ResourceRegistry registry;

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
        when(clusterDiscovery.discoveryNodes()).thenReturn(ImmutableSet.of());
        when(transportService.channel(node1)).thenReturn(transportChannel1);
        when(transportService.channel(node2)).thenReturn(transportChannel2);
        when(transportService.channel(node3)).thenReturn(transportChannel3);
        when(transportService.channel(node4)).thenReturn(transportChannel4);
        when(transportService.channel(node5)).thenReturn(transportChannel5);
        when(registry.apply(TestFSMMessage.INSTANCE)).thenReturn(TestFSMMessage.INSTANCE);

        context = new TestRaftContext();
        config = ConfigFactory.defaultReference();
        override("node.name", "test");
        override("node.path", testFolder.getRoot().getAbsolutePath());
        override("raft.enabled", "true");
        override("raft.bootstrap", "true");
        override("raft.snapshot-interval", "100");
        injector = Guice.createInjector(
                new ComponentModule(config),
                new StreamModule(),
                new FileModule(),
                new TestRaftModule()
        );
        config = injector.getInstance(Config.class);
        injector.getInstance(LifecycleService.class).start();
        persistentService = injector.getInstance(PersistentService.class);
        log = persistentService.openLog();
    }

    private void appendClusterSelf() throws Exception {
        log = log.append(new LogEntry(new StableClusterConfiguration(node1), 1, 1, node1)).commit(1);
    }

    private void appendClusterConf() throws Exception {
        log = log.append(new LogEntry(new StableClusterConfiguration(node1, node2, node3), 1, 1, node1)).commit(1);
    }

    private void appendBigClusterConf() throws Exception {
        log = log.append(new LogEntry(new StableClusterConfiguration(node1, node2, node3, node4, node5), 1, 1, node1)).commit(1);
    }

    private void override(String key, String value) {
        config = ConfigFactory.parseMap(ImmutableMap.of(key, value)).withFallback(config);
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
        expectLeader();
    }

    @Test
    public void testJointCluster() throws Exception {
        override("raft.bootstrap", "false");
        when(clusterDiscovery.discoveryNodes()).thenReturn(ImmutableSet.of(node1, node2, node3));
        start();
        expectFollower();
        Assert.assertEquals(ImmutableSet.of(node1, node2, node3), clusterDiscovery.discoveryNodes());
        verify(transportChannel2).message(new AddServer(node1));
        verify(transportChannel3).message(new AddServer(node1));
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
        raft.apply(new ClientMessage(node2, Noop.INSTANCE));
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
    }

    @Test
    public void testFollowerSendClientMessageToRecentLeader() throws Exception {
        appendClusterConf();
        start();
        expectFollower();
        expectTerm(1);
        raft.apply(appendEntries(node2, 1, 1, 1, 1));
        Assert.assertEquals(Optional.of(node2), raft.recentLeader());

        raft.apply(new ClientMessage(node2, Noop.INSTANCE));
        Assert.assertEquals(ImmutableList.of(), raft.currentStashed());
        verify(transportChannel2).message(new ClientMessage(node2, Noop.INSTANCE));
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
        Assert.assertEquals(ImmutableSet.of(), clusterDiscovery.discoveryNodes());
        raft.apply(ElectionTimeout.INSTANCE);
        expectFollower();
    }

    @Test
    public void testFollowerRejectVoteIfLastLogTermIsOld() throws Exception {
        appendClusterConf();
        start();
        requestVote(1, node2, 0, 0);
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerRejectVoteIfLastLogIndexIsOld() throws Exception {
        appendClusterConf();
        start();
        requestVote(1, node2, 1, 0);
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerRejectVoteIfAlreadyVoted() throws Exception {
        appendClusterConf();
        start();
        requestVote(1, node2, 1, 1);
        requestVote(1, node3, 1, 1);
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testFollowerRejectVoteIfTermOld() throws Exception {
        appendClusterConf();
        start();
        requestVote(0, node2, 1, 1);
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerAcceptVoteIfTermIsNewer() throws Exception {
        appendClusterConf();
        start();
        requestVote(2, node2, 1, 1);
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
        expectTerm(2);
    }

    @Test
    public void testFollowerRejectAppendEntriesIfTermIsOld() throws Exception {
        appendClusterConf();
        start();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, 1, 1, node2);
        raft.apply(appendEntries(node2, 1, 1, 0, 0, logEntry));
        Assert.assertFalse(raft.replicatedLog().contains(logEntry));
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndTermNotMatch() throws Exception {
        appendClusterConf();
        start();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, 1, 1, node2);
        raft.apply(appendEntries(node2, 0, 1, 0, 0, logEntry));
        Assert.assertFalse(raft.replicatedLog().contains(logEntry));
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndIndexNotMatch() throws Exception {
        appendClusterConf();
        start();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, 1, 1, node2);
        raft.apply(appendEntries(node2, 1, 0, 1, 0, logEntry));
        Assert.assertFalse(raft.replicatedLog().contains(logEntry));
    }

    @Test
    public void testFollowerAppendEntriesIfLogEmpty() throws Exception {
        appendClusterConf();
        start();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, 1, 2, node2);
        raft.apply(appendEntries(node2, 1, 1, 1, 0, logEntry));
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
    public void testFollowerIgnoreSendHeartbeat() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(SendHeartbeat.INSTANCE);
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
        RaftSnapshot snapshot = new RaftSnapshot(metadata, ImmutableList.of());
        raft.apply(new InstallSnapshot(node4, 0, snapshot));
        expectFollower();
        expectTerm(1);
        verify(transportChannel4).message(new InstallSnapshotRejected(node1, 1));
    }

    @Test
    public void testFollowerInstallSnapshotIfTermIsNewer() throws Exception {
        appendClusterSelf();
        start();
        ClusterConfiguration conf = new StableClusterConfiguration(node1);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        RaftSnapshot snapshot = new RaftSnapshot(metadata, ImmutableList.of());
        raft.apply(new InstallSnapshot(node4, 2, snapshot));
        expectFollower();
        expectTerm(2);
        verify(transportChannel4).message(new InstallSnapshotSuccessful(node1, 2, 1));
    }

    @Test
    public void testFollowerInstallSnapshotIfTermIsEqual() throws Exception {
        appendClusterSelf();
        start();
        ClusterConfiguration conf = new StableClusterConfiguration(node1);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        RaftSnapshot snapshot = new RaftSnapshot(metadata, ImmutableList.of());
        raft.apply(new InstallSnapshot(node4, 1, snapshot));
        expectFollower();
        expectTerm(1);
        verify(transportChannel4).message(new InstallSnapshotSuccessful(node1, 1, 1));
    }

    @Test
    public void testFollowerRejectAddServer() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(new AddServer(node4));
        verify(transportChannel4).message(new AddServerResponse(AddServerResponse.Status.NOT_LEADER, Optional.empty()));
    }

    @Test
    public void testFollowerRejectRemoveServer() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(new RemoveServer(node4));
        verify(transportChannel4).message(new RemoveServerResponse(RemoveServerResponse.Status.NOT_LEADER, Optional.empty()));
    }

    // candidate

    @Test
    public void testCandidateRejectAddServer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(new AddServer(node4));
        verify(transportChannel4).message(new AddServerResponse(AddServerResponse.Status.NOT_LEADER, Optional.empty()));
    }

    @Test
    public void testCandidateRejectRemoveServer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(new RemoveServer(node3));
        verify(transportChannel3).message(new RemoveServerResponse(RemoveServerResponse.Status.NOT_LEADER, Optional.empty()));
    }

    @Test
    public void testCandidateRejectVoteIfLastLogTermIsOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        requestVote(1, node2, 0, 0);
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfTermEqual() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        requestVote(2, node2, 1, 1);
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfLastLogIndexIsOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        requestVote(3, node2, 1, 0);
        expectFollower();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
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
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfTermOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        requestVote(0, node2, 1, 1);
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateVoteIfTermIsNewer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        requestVote(3, node2, 1, 1);
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
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
        Assert.assertFalse(raft.currentMeta().hasMajority());
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
        raft.apply(new ClientMessage(node2, Noop.INSTANCE));
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
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
        LogEntry entry = new LogEntry(Noop.INSTANCE, 1, 2, node2);
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
        RaftSnapshot snapshot = new RaftSnapshot(metadata, ImmutableList.of());
        raft.apply(new InstallSnapshot(node4, 2, snapshot));
        expectFollower();
        expectTerm(2);
        verify(transportChannel4).message(new InstallSnapshotSuccessful(node1, 2, 1));
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
        RaftSnapshot snapshot = new RaftSnapshot(metadata, ImmutableList.of());
        raft.apply(new InstallSnapshot(node4, 3, snapshot));
        expectFollower();
        expectTerm(3);
        verify(transportChannel4).message(new InstallSnapshotSuccessful(node1, 3, 1));
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
        RaftSnapshot snapshot = new RaftSnapshot(metadata, ImmutableList.of());
        raft.apply(new InstallSnapshot(node4, 1, snapshot));
        expectCandidate();
        expectTerm(2);
        verify(transportChannel4).message(new InstallSnapshotRejected(node1, 2));
    }

    @Test
    public void testCandidateIgnoreAddServerResponse() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        expectTerm(2);
        raft.apply(new AddServerResponse(AddServerResponse.Status.OK, Optional.empty()));
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
        raft.apply(new RemoveServerResponse(RemoveServerResponse.Status.OK, Optional.empty()));
        expectCandidate();
        expectTerm(2);
    }

    // leader

    private void becameLeader() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        verify(transportChannel2).message(new RequestVote(2, node1, 1, 1));
        verify(transportChannel3).message(new RequestVote(2, node1, 1, 1));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
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
                raft.replicatedLog().entries()
        );
    }

    @Test
    public void testLeaderSendHeartbeatOnElection() throws Exception {
        becameLeader();
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
    }

    @Test
    public void testLeaderSendHeartbeatOnSendHeartbeat() throws Exception {
        becameLeader();
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        raft.apply(new AppendSuccessful(node2, 2, 2));
        raft.apply(new AppendSuccessful(node3, 2, 2));

        raft.apply(new ClientMessage(node2, Noop.INSTANCE));
        raft.apply(SendHeartbeat.INSTANCE);

        verify(transportChannel2).message(appendEntries(node1, 2, 2, 2, 2, noop(2, 3, node2)));
        verify(transportChannel3).message(appendEntries(node1, 2, 2, 2, 2, noop(2, 3, node2)));
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
        log = log.append(noop(1, 2, node1)).append(noop(1, 3, node1));
        start();
        electionTimeout();
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);

        verify(transportChannel2).message(appendEntries(node1, 2, 1, 3, 0, noop(2, 4, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 3, 0, noop(2, 4, node1)));

        raft.apply(new AppendRejected(node2, 2, 4));
        raft.apply(new AppendRejected(node3, 2, 4));

        // entry 4 does not included because different term
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 2, 0, noop(1, 3, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 2, 0, noop(1, 3, node1)));

        raft.apply(new AppendRejected(node2, 2, 4));
        raft.apply(new AppendRejected(node3, 2, 4));

        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 0, noop(1, 2, node1), noop(1, 3, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 0, noop(1, 2, node1), noop(1, 3, node1)));
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
        raft.apply(new InstallSnapshot(node2, 1, new RaftSnapshot(metadata, ImmutableList.of())));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderRejectInstallSnapshotIfTermIsEqual() throws Exception {
        becameLeader();
        StableClusterConfiguration conf = new StableClusterConfiguration(node1, node2, node3);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        raft.apply(new InstallSnapshot(node2, 2, new RaftSnapshot(metadata, ImmutableList.of())));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotIfTermIsNewer() throws Exception {
        becameLeader();
        StableClusterConfiguration conf = new StableClusterConfiguration(node1, node2, node3);
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(1, 1, conf);
        raft.apply(new InstallSnapshot(node2, 3, new RaftSnapshot(metadata, ImmutableList.of())));
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
        log = log.append(noop(1, 2, node1)).append(noop(1, 3, node1));
        start();
        electionTimeout();
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        raft.apply(new InstallSnapshotRejected(node2, 2));
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 3, 0, noop(2, 4, node1)));
    }

    @Test
    public void testLeaderUnstashClientMessages() throws Exception {
        appendClusterConf();
        start();
        expectFollower();
        raft.apply(new ClientMessage(node2, Noop.INSTANCE));
        electionTimeout();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        expectLeader();
        Assert.assertEquals(ImmutableList.of(), raft.currentStashed());
    }

    @Test
    public void testLeaderSendResponseToRemoteClient() throws Exception {
        becameLeader();
        raft.apply(new AppendSuccessful(node2, 2, 2));
        raft.apply(new AppendSuccessful(node3, 2, 2));

        raft.apply(new ClientMessage(node2, TestFSMMessage.INSTANCE));
        raft.apply(new AppendSuccessful(node2, 2, 3));
        raft.apply(new AppendSuccessful(node3, 2, 3));

        Assert.assertEquals(3, raft.replicatedLog().committedIndex());
        verify(transportChannel2).message(TestFSMMessage.INSTANCE);
    }

    @Test
    public void testLeaderSendResponseToLocalClient() throws Exception {
        becameLeader();
        raft.apply(new AppendSuccessful(node2, 2, 2));
        raft.apply(new AppendSuccessful(node3, 2, 2));

        raft.apply(new ClientMessage(node1, TestFSMMessage.INSTANCE));
        raft.apply(new AppendSuccessful(node2, 2, 3));
        raft.apply(new AppendSuccessful(node3, 2, 3));

        Assert.assertEquals(3, raft.replicatedLog().committedIndex());
        verify(transportController).dispatch(new MessageTransportFrame(Version.CURRENT, TestFSMMessage.INSTANCE));
    }

    @Test
    public void testLeaderRejectVoteIfTermIsEqual() throws Exception {
        becameLeader();
        requestVote(2, node2, 2, 2);
        expectLeader();
        expectTerm(2);
        verify(transportChannel2).message(new DeclineCandidate(node1, 2));
    }

    @Test
    public void testLeaderVoteCandidateIfTermIsNewer() throws Exception {
        becameLeader();
        requestVote(3, node2, 2, 2);
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
        verify(transportChannel2).message(new VoteCandidate(node1, 3));
    }

    @Test
    public void testLeaderCreateSnapshot() throws Exception {
        // snapshot
        RaftSnapshotMetadata meta = new RaftSnapshotMetadata(2, 100, new StableClusterConfiguration(node1, node2, node3));
        RaftSnapshot snapshot = new RaftSnapshot(meta, ImmutableList.of());
        when(registry.prepareSnapshot(meta)).thenReturn(snapshot);

        becameLeader();
        for (int i = 0; i < 100; i++) {
            raft.apply(new ClientMessage(node1, Noop.INSTANCE));
        }

        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 0, new LogEntry(Noop.INSTANCE, 2, 2, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 0, new LogEntry(Noop.INSTANCE, 2, 2, node1)));
        // commit index 1
        appendSuccessful(node2, 2, 100);
        appendSuccessful(node3, 2, 100);

        ReplicatedLog log = raft.replicatedLog();
        logger.info("log: {}", log);
        Assert.assertTrue(log.hasSnapshot());
        Assert.assertEquals(meta, log.snapshot().getMeta());

        // send install snapshot
        raft.apply(new AppendRejected(node3, 2, 1));
        verify(transportChannel3).message(new InstallSnapshot(node1, 2, snapshot));
    }

    // joint consensus

    @Test
    public void testAddServer() throws Exception {
        becameLeader();
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        appendSuccessful(node2, 2, 2);
        appendSuccessful(node3, 2, 2);

        raft.apply(new AddServer(node4));
        ClusterConfiguration stable = new StableClusterConfiguration(node1, node2, node3, node4);
        LogEntry stableEntry = new LogEntry(stable, 2, 3, node4);
        verify(transportChannel2).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportChannel3).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        appendSuccessful(node2, 2, 3);
        appendSuccessful(node3, 2, 3);
        Assert.assertEquals(stable, raft.currentMeta().getConfig());
    }

    @Test
    public void testAddServerInTransitioningState() throws Exception {
        becameLeader();
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        appendSuccessful(node2, 2, 2);
        appendSuccessful(node3, 2, 2);

        raft.apply(new AddServer(node4));
        ClusterConfiguration stable = new StableClusterConfiguration(node1, node2, node3, node4);
        LogEntry stableEntry = new LogEntry(stable, 2, 3, node4);
        verify(transportChannel2).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportChannel3).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        expectLeader();
        Assert.assertTrue(raft.currentMeta().getConfig().isTransitioning());

        raft.apply(new AddServer(node5));
        verify(transportChannel5).message(new AddServerResponse(AddServerResponse.Status.TIMEOUT, Optional.of(node1)));
    }

    @Test
    public void testRemoveServer() throws Exception {
        becameLeader();
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        appendSuccessful(node2, 2, 2);
        appendSuccessful(node3, 2, 2);

        raft.apply(new RemoveServer(node3));
        ClusterConfiguration stable = new StableClusterConfiguration(node1, node2);
        LogEntry stableEntry = new LogEntry(stable, 2, 3, node3);
        verify(transportChannel2).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportChannel3).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        appendSuccessful(node2, 2, 3);
        appendSuccessful(node3, 2, 3);
        Assert.assertEquals(stable, raft.currentMeta().getConfig());
    }

    @Test
    public void testRemoveServerInTransitioningState() throws Exception {
        becameLeader();
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        appendSuccessful(node2, 2, 2);
        appendSuccessful(node3, 2, 2);

        raft.apply(new RemoveServer(node3));
        ClusterConfiguration stable = new StableClusterConfiguration(node1, node2);
        LogEntry stableEntry = new LogEntry(stable, 2, 3, node3);
        verify(transportChannel2).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportChannel3).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        expectLeader();
        Assert.assertTrue(raft.currentMeta().getConfig().isTransitioning());

        raft.apply(new RemoveServer(node2));
        verify(transportChannel2).message(new RemoveServerResponse(RemoveServerResponse.Status.TIMEOUT, Optional.of(node1)));
    }

    @Test
    public void testLeaderRemoveSelf() throws Exception {
        becameLeader();
        verify(transportChannel2).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        verify(transportChannel3).message(appendEntries(node1, 2, 1, 1, 0, noop(2, 2, node1)));
        appendSuccessful(node2, 2, 2);
        appendSuccessful(node3, 2, 2);

        raft.apply(new RemoveServer(node1));
        ClusterConfiguration stable = new StableClusterConfiguration(node2, node3);
        LogEntry stableEntry = new LogEntry(stable, 2, 3, node1);
        verify(transportChannel2).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportChannel3).message(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        appendSuccessful(node2, 2, 3);
        appendSuccessful(node3, 2, 3);
        Assert.assertEquals(stable, raft.currentMeta().getConfig());
        expectFollower();
    }

    @Test
    public void testFollowerHandleAddServerResponse() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, 1, 1, node1)).commit(1);
        start();
        expectFollower();
        raft.apply(new AddServerResponse(AddServerResponse.Status.OK, Optional.of(node2)));
        Assert.assertEquals(node2, raft.recentLeader().orElse(null));
    }

    @Test
    public void testFollowerHandleRemoveServerResponse() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, 1, 1, node1)).commit(1);
        start();
        expectFollower();
        raft.apply(new RemoveServerResponse(RemoveServerResponse.Status.OK, Optional.of(node2)));
        Assert.assertEquals(node2, raft.recentLeader().orElse(null));
    }

    // additional methods

    private void requestVote(long term, DiscoveryNode node, long lastLogTerm, long lastLogIndex) throws IOException {
        raft.apply(new RequestVote(term, node, lastLogTerm, lastLogIndex));
    }

    private void voteCandidate(DiscoveryNode node, long term) throws IOException {
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
        raft.apply(ElectionTimeout.INSTANCE);
    }

    private LogEntry noop(long term, long index, DiscoveryNode client) {
        return new LogEntry(Noop.INSTANCE, term, index, client);
    }

    @SuppressWarnings("SameParameterValue")
    private LogEntry stable(long term, long index, DiscoveryNode... nodes) {
        return new LogEntry(new StableClusterConfiguration(nodes), term, index, node1);
    }

    @SuppressWarnings("SameParameterValue")
    private void appendSuccessful(DiscoveryNode node, long term, long index) throws IOException {
        raft.apply(new AppendSuccessful(node, term, index));
    }

    private AppendEntries appendEntries(DiscoveryNode node, long term, long prevTerm, long prevIndex, long commit, LogEntry... logEntries) {
        return new AppendEntries(node, term, prevTerm, prevIndex, ImmutableList.copyOf(logEntries), commit);
    }

    // test dependencies

    private static class TestFSMMessage implements Streamable {

        public static final TestFSMMessage INSTANCE = new TestFSMMessage();

        private TestFSMMessage() {
        }

        @SuppressWarnings("unused")
        public static TestFSMMessage read(StreamInput stream) throws IOException {
            return INSTANCE;
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
        }
    }

    private class TestRaftContext implements RaftContext {

        @Override
        public ScheduledFuture schedule(Runnable task, long timeout, TimeUnit timeUnit) {
            return new TestScheduledFuture();
        }

        @Override
        public ScheduledFuture scheduleAtFixedRate(Runnable task, long delay, long timeout, TimeUnit timeUnit) {
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

            Multibinder<StreamableRegistry> streamableBinder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendEntries.class, AppendEntries::new, 200));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendRejected.class, AppendRejected::new, 201));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendSuccessful.class, AppendSuccessful::new, 202));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(ClientMessage.class, ClientMessage::new, 210));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(ElectionTimeout.class, ElectionTimeout::read, 220));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RequestVote.class, RequestVote::new, 223));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(VoteCandidate.class, VoteCandidate::new, 225));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(DeclineCandidate.class, DeclineCandidate::new, 226));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(SendHeartbeat.class, SendHeartbeat::read, 224));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(LogEntry.class, LogEntry::new, 230));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(Noop.class, Noop::read, 231));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftSnapshot.class, RaftSnapshot::new, 240));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftSnapshotMetadata.class, RaftSnapshotMetadata::new, 241));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshot.class, InstallSnapshot::new, 261));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshotRejected.class, InstallSnapshotRejected::new, 262));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshotSuccessful.class, InstallSnapshotSuccessful::new, 263));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(JointConsensusClusterConfiguration.class, JointConsensusClusterConfiguration::new, 270));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(StableClusterConfiguration.class, StableClusterConfiguration::new, 271));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AddServer.class, AddServer::new, 280));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AddServerResponse.class, AddServerResponse::new, 281));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RemoveServer.class, RemoveServer::new, 282));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RemoveServerResponse.class, RemoveServerResponse::new, 283));

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(TestFSMMessage.class, TestFSMMessage::read, 290));
        }
    }
}
