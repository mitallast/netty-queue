package org.mitallast.queue.raft;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.Message;
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
import org.mitallast.queue.common.proto.*;
import org.mitallast.queue.proto.raft.*;
import org.mitallast.queue.proto.test.*;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.persistent.FilePersistentService;
import org.mitallast.queue.raft.persistent.PersistentService;
import org.mitallast.queue.raft.persistent.ReplicatedLog;
import org.mitallast.queue.raft.resource.ResourceRegistry;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

    private final ProtoService protoService = new ProtoService(ImmutableSet.of(
        new ProtoRegistry(1000, AddServer.getDescriptor(), AddServer.parser()),
        new ProtoRegistry(1001, AddServerResponse.getDescriptor(), AddServerResponse.parser()),
        new ProtoRegistry(1002, AppendEntries.getDescriptor(), AppendEntries.parser()),
        new ProtoRegistry(1003, AppendRejected.getDescriptor(), AppendRejected.parser()),
        new ProtoRegistry(1004, AppendSuccessful.getDescriptor(), AppendSuccessful.parser()),
        new ProtoRegistry(1005, ClientMessage.getDescriptor(), ClientMessage.parser()),
        new ProtoRegistry(1006, DeclineCandidate.getDescriptor(), DeclineCandidate.parser()),
        new ProtoRegistry(1007, DiscoveryNode.getDescriptor(), DiscoveryNode.parser()),
        new ProtoRegistry(1008, InstallSnapshot.getDescriptor(), InstallSnapshot.parser()),
        new ProtoRegistry(1009, InstallSnapshotRejected.getDescriptor(), InstallSnapshotRejected.parser()),
        new ProtoRegistry(1010, InstallSnapshotSuccessful.getDescriptor(), InstallSnapshotSuccessful.parser()),
        new ProtoRegistry(1011, ClusterConfiguration.getDescriptor(), ClusterConfiguration.parser()),
        new ProtoRegistry(1012, JointConsensusClusterConfiguration.getDescriptor(), JointConsensusClusterConfiguration.parser()),
        new ProtoRegistry(1013, StableClusterConfiguration.getDescriptor(), StableClusterConfiguration.parser()),
        new ProtoRegistry(1014, LogEntry.getDescriptor(), LogEntry.parser()),
        new ProtoRegistry(1015, Noop.getDescriptor(), Noop.parser()),
        new ProtoRegistry(1016, RaftSnapshot.getDescriptor(), RaftSnapshot.parser()),
        new ProtoRegistry(1017, RaftSnapshotMetadata.getDescriptor(), RaftSnapshotMetadata.parser()),
        new ProtoRegistry(1018, RemoveServer.getDescriptor(), RemoveServer.parser()),
        new ProtoRegistry(1019, RemoveServerResponse.getDescriptor(), RemoveServerResponse.parser()),
        new ProtoRegistry(1020, RequestVote.getDescriptor(), RequestVote.parser()),
        new ProtoRegistry(1021, VoteCandidate.getDescriptor(), VoteCandidate.parser()),

        new ProtoRegistry(2000, TestFSMMessage.getDescriptor(), TestFSMMessage.parser())
    ));

    private TestRaftContext context;

    private Config config;

    private Raft raft;

    private PersistentService persistentService;
    private ReplicatedLog log;

    private DiscoveryNode node1 = DiscoveryNode.newBuilder().setHost("localhost").setPort(8801).build();
    private DiscoveryNode node2 = DiscoveryNode.newBuilder().setHost("localhost").setPort(8802).build();
    private DiscoveryNode node3 = DiscoveryNode.newBuilder().setHost("localhost").setPort(8803).build();
    private DiscoveryNode node4 = DiscoveryNode.newBuilder().setHost("localhost").setPort(8804).build();
    private DiscoveryNode node5 = DiscoveryNode.newBuilder().setHost("localhost").setPort(8805).build();


    private AppendEntries appendEntries(DiscoveryNode node, long term, long prevTerm, long prevIndex, long commit, LogEntry... logEntries) {
        return AppendEntries.newBuilder()
            .setMember(node)
            .setTerm(term)
            .setPrevLogTerm(prevTerm)
            .setPrevLogIndex(prevIndex)
            .setLeaderCommit(commit)
            .addAllEntries(Arrays.asList(logEntries))
            .build();
    }

    private AppendRejected appendRejected(DiscoveryNode member, long term, long lastIndex) {
        return AppendRejected.newBuilder()
            .setMember(member)
            .setTerm(term)
            .setLastIndex(lastIndex)
            .build();
    }

    private DeclineCandidate declineCandidate(DiscoveryNode member, long term) {
        return DeclineCandidate.newBuilder()
            .setMember(member)
            .setTerm(term)
            .build();
    }

    private AppendSuccessful appendSuccessful(DiscoveryNode member, long term, long index) {
        return AppendSuccessful.newBuilder()
            .setMember(member)
            .setTerm(term)
            .setLastIndex(index)
            .build();
    }

    private RequestVote requestVote(DiscoveryNode candidate, long term, long lastLogTerm, long lastLogIndex) {
        return RequestVote.newBuilder()
            .setCandidate(candidate)
            .setTerm(term)
            .setLastLogTerm(lastLogTerm)
            .setLastLogIndex(lastLogIndex)
            .build();
    }

    private VoteCandidate voteCandidate(DiscoveryNode member, long term) {
        return VoteCandidate.newBuilder()
            .setMember(member)
            .setTerm(term)
            .build();
    }

    private Noop noopEntry() {
        return Noop.newBuilder().build();
    }

    private ClientMessage clientMessage(DiscoveryNode client, Message message) {
        return ClientMessage.newBuilder()
            .setClient(client)
            .setCommand(protoService.pack(message))
            .build();
    }

    private AddServer addServer(DiscoveryNode node) {
        return AddServer.newBuilder()
            .setMember(node)
            .build();
    }

    private AddServerResponse addServerResponse(AddServerResponse.Status status) {
        return AddServerResponse.newBuilder()
            .setStatus(status)
            .build();
    }

    private AddServerResponse addServerResponse(AddServerResponse.Status status, DiscoveryNode leader) {
        return AddServerResponse.newBuilder()
            .setStatus(status)
            .setLeader(leader)
            .build();
    }

    private RemoveServerResponse removeServerResponse(RemoveServerResponse.Status status) {
        return RemoveServerResponse.newBuilder()
            .setStatus(status)
            .build();
    }

    private RemoveServerResponse removeServerResponse(RemoveServerResponse.Status status, DiscoveryNode leader) {
        return RemoveServerResponse.newBuilder()
            .setStatus(status)
            .setLeader(leader)
            .build();
    }

    private RemoveServer removeServer(DiscoveryNode node) {
        return RemoveServer.newBuilder()
            .setMember(node)
            .build();
    }

    private StableClusterConfiguration stable(DiscoveryNode... members) {
        return StableClusterConfiguration.newBuilder()
            .addAllMembers(Arrays.asList(members))
            .build();
    }

    private ClusterConfiguration config(StableClusterConfiguration stable) {
        return ClusterConfiguration.newBuilder()
            .setStable(stable)
            .build();
    }

    private ClusterConfiguration configStable(DiscoveryNode... members) {
        return config(stable(members));
    }

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

    private InstallSnapshotSuccessful installSnapshotSuccessful(DiscoveryNode member, long term, long index) {
        return InstallSnapshotSuccessful.newBuilder()
            .setMember(member)
            .setTerm(term)
            .setLastIndex(index)
            .build();
    }

    private InstallSnapshotRejected installSnapshotRejected(DiscoveryNode member, long term) {
        return InstallSnapshotRejected.newBuilder()
            .setMember(member)
            .setTerm(term)
            .build();
    }

    private InstallSnapshot installSnapshot(DiscoveryNode leader, long term, RaftSnapshot snapshot) {
        return InstallSnapshot.newBuilder()
            .setLeader(leader)
            .setTerm(term)
            .setSnapshot(snapshot)
            .build();
    }

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
        when(registry.apply(TestFSMMessage.getDefaultInstance())).thenReturn(TestFSMMessage.getDefaultInstance());

        context = new TestRaftContext();
        config = ConfigFactory.defaultReference();
        override("node.name", "test");
        override("node.path", testFolder.getRoot().getAbsolutePath());
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
        log = log.append(logEntry(configStable(node1), 1, 1, node1)).commit(1);
    }

    private void appendClusterConf() throws Exception {
        log = log.append(logEntry(configStable(node1, node2, node3), 1, 1, node1)).commit(1);
    }

    private void appendBigClusterConf() throws Exception {
        log = log.append(logEntry(configStable(node1, node2, node3, node4, node5), 1, 1, node1)).commit(1);
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
            protoService,
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
        expectLeader();
    }

    @Test
    public void testJointCluster() throws Exception {
        override("raft.bootstrap", "false");
        when(clusterDiscovery.discoveryNodes()).thenReturn(ImmutableSet.of(node1, node2, node3));
        start();
        expectFollower();
        Assert.assertEquals(ImmutableSet.of(node1, node2, node3), clusterDiscovery.discoveryNodes());
        verify(transportChannel2).send(addServer(node1));
        verify(transportChannel3).send(addServer(node1));
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
        raft.apply(clientMessage(node2, noopEntry()));
        Assert.assertEquals(ImmutableList.of(clientMessage(node2, noopEntry())), raft.currentStashed());
    }

    @Test
    public void testFollowerSendClientMessageToRecentLeader() throws Exception {
        appendClusterConf();
        start();
        expectFollower();
        expectTerm(1);
        raft.apply(appendEntries(node2, 1, 1, 1, 1));
        Assert.assertEquals(Optional.of(node2), raft.recentLeader());

        raft.apply(clientMessage(node2, noopEntry()));
        Assert.assertEquals(ImmutableList.of(), raft.currentStashed());
        verify(transportChannel2).send(clientMessage(node2, noopEntry()));
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
        electionTimeout();
        expectFollower();
    }

    @Test
    public void testFollowerRejectVoteIfLastLogTermIsOld() throws Exception {
        appendClusterConf();
        start();
        applyRequestVote(1, node2, 0, 0);
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerRejectVoteIfLastLogIndexIsOld() throws Exception {
        appendClusterConf();
        start();
        applyRequestVote(1, node2, 1, 0);
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerRejectVoteIfAlreadyVoted() throws Exception {
        appendClusterConf();
        start();
        applyRequestVote(1, node2, 1, 1);
        applyRequestVote(1, node3, 1, 1);
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testFollowerRejectVoteIfTermOld() throws Exception {
        appendClusterConf();
        start();
        applyRequestVote(0, node2, 1, 1);
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerAcceptVoteIfTermIsNewer() throws Exception {
        appendClusterConf();
        start();
        applyRequestVote(2, node2, 1, 1);
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
        expectTerm(2);
    }

    @Test
    public void testFollowerRejectAppendEntriesIfTermIsOld() throws Exception {
        appendClusterConf();
        start();
        LogEntry logEntry = logEntry(noopEntry(), 1, 1, node2);
        raft.apply(appendEntries(node2, 1, 1, 0, 0, logEntry));
        Assert.assertFalse(raft.replicatedLog().contains(logEntry));
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndTermNotMatch() throws Exception {
        appendClusterConf();
        start();
        LogEntry logEntry = logEntry(noopEntry(), 1, 1, node2);
        raft.apply(appendEntries(node2, 0, 1, 0, 0, logEntry));
        Assert.assertFalse(raft.replicatedLog().contains(logEntry));
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndIndexNotMatch() throws Exception {
        appendClusterConf();
        start();
        LogEntry logEntry = logEntry(noopEntry(), 1, 1, node2);
        raft.apply(appendEntries(node2, 1, 0, 1, 0, logEntry));
        Assert.assertFalse(raft.replicatedLog().contains(logEntry));
    }

    @Test
    public void testFollowerAppendEntriesIfLogEmpty() throws Exception {
        appendClusterConf();
        start();
        LogEntry logEntry = logEntry(noopEntry(), 1, 2, node2);
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
        applyAppendSuccessful(node2, 2, 2);
        expectFollower();
    }

    @Test
    public void testFollowerIgnoreAppendRejected() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(appendRejected(node2, 1, 1));
        expectFollower();
    }

    @Test
    public void testFollowerIgnoreDeclineCandidate() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(declineCandidate(node2, 1));
        expectFollower();
        expectTerm(1);
    }

    @Test
    public void testFollowerIgnoreInstallSnapshotSuccessful() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(installSnapshotSuccessful(node2, 2, 1));
        expectFollower();
        expectTerm(1);
    }

    @Test
    public void testFollowerIgnoreInstallInstallSnapshotRejected() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(installSnapshotRejected(node2, 2));
        expectFollower();
        expectTerm(1);
    }

    @Test
    public void testFollowerRejectInstallSnapshotIfTermIsOld() throws Exception {
        appendClusterSelf();
        start();
        ClusterConfiguration conf = configStable(node1);
        RaftSnapshotMetadata metadata = snapshotMeta(1, 1, conf);
        RaftSnapshot snapshot = snapshot(metadata);
        raft.apply(installSnapshot(node4, 0, snapshot));
        expectFollower();
        expectTerm(1);
        verify(transportChannel4).send(installSnapshotRejected(node1, 1));
    }

    @Test
    public void testFollowerInstallSnapshotIfTermIsNewer() throws Exception {
        appendClusterSelf();
        start();
        ClusterConfiguration conf = configStable(node1);
        RaftSnapshotMetadata metadata = snapshotMeta(1, 1, conf);
        RaftSnapshot snapshot = snapshot(metadata);
        raft.apply(installSnapshot(node4, 2, snapshot));
        expectFollower();
        expectTerm(2);
        verify(transportChannel4).send(installSnapshotSuccessful(node1, 2, 1));
    }

    @Test
    public void testFollowerInstallSnapshotIfTermIsEqual() throws Exception {
        appendClusterSelf();
        start();
        ClusterConfiguration conf = configStable(node1);
        RaftSnapshotMetadata metadata = snapshotMeta(1, 1, conf);
        RaftSnapshot snapshot = snapshot(metadata);
        raft.apply(installSnapshot(node4, 1, snapshot));
        expectFollower();
        expectTerm(1);
        verify(transportChannel4).send(installSnapshotSuccessful(node1, 1, 1));
    }

    @Test
    public void testFollowerRejectAddServer() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(addServer(node4));
        verify(transportChannel4).send(addServerResponse(AddServerResponse.Status.NOT_LEADER));
    }

    @Test
    public void testFollowerRejectRemoveServer() throws Exception {
        appendClusterSelf();
        start();
        raft.apply(removeServer(node4));
        verify(transportChannel4).send(removeServerResponse(RemoveServerResponse.Status.NOT_LEADER));
    }

    // candidate

    @Test
    public void testCandidateRejectAddServer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(addServer(node4));
        verify(transportChannel4).send(addServerResponse(AddServerResponse.Status.NOT_LEADER));
    }

    @Test
    public void testCandidateRejectRemoveServer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(removeServer(node3));
        verify(transportChannel3).send(removeServerResponse(RemoveServerResponse.Status.NOT_LEADER));
    }

    @Test
    public void testCandidateRejectVoteIfLastLogTermIsOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        applyRequestVote(1, node2, 0, 0);
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfTermEqual() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        applyRequestVote(2, node2, 1, 1);
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfLastLogIndexIsOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        applyRequestVote(3, node2, 1, 0);
        expectFollower();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testCandidateRejectVoteIfAlreadyVoted() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        applyRequestVote(3, node2, 1, 1);
        expectFollower();
        applyRequestVote(3, node3, 1, 1);
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfTermOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        applyRequestVote(0, node2, 1, 1);
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateVoteIfTermIsNewer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        applyRequestVote(3, node2, 1, 1);
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
        expectTerm(3);
    }

    @Test
    public void testCandidateVoteRejectIfTermOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        applyVoteCandidate(node2, 1);
        expectCandidate();
    }

    @Test
    public void testCandidateVoteRejectIfTermNewer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        applyVoteCandidate(node2, 3);
        expectFollower();
        expectTerm(3);
    }

    @Test
    public void testCandidateVotedHasNotMajority() throws Exception {
        appendBigClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        applyVoteCandidate(node2, 2);
        expectCandidate();
        Assert.assertFalse(raft.currentMeta().hasMajority());
    }

    @Test
    public void testCandidateVotedHasMajority() throws Exception {
        appendBigClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        applyVoteCandidate(node2, 2);
        applyVoteCandidate(node3, 2);
        expectLeader();
        Assert.assertFalse(raft.currentMeta().hasMajority());
    }

    @Test
    public void testCandidateDeclineCandidateWithTermNewer() throws Exception {
        appendBigClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(declineCandidate(node2, 3));
        expectFollower();
        expectTerm(3);
    }

    @Test
    public void testCandidateDeclineCandidateWithEqualTerm() throws Exception {
        appendBigClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(declineCandidate(node2, 2));
        expectCandidate();
    }

    @Test
    public void testCandidateDeclineCandidateWithTermOld() throws Exception {
        appendBigClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        raft.apply(declineCandidate(node2, 1));
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
        raft.apply(clientMessage(node2, noopEntry()));
        Assert.assertEquals(ImmutableList.of(clientMessage(node2, noopEntry())), raft.currentStashed());
    }

    @Test
    public void testCandidateBecameLeaderOnVoteMajority() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        applyVoteCandidate(node2, 2);
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
        LogEntry entry = logEntry(noopEntry(), 1, 2, node2);
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
        ClusterConfiguration conf = configStable(node1);
        RaftSnapshotMetadata metadata = snapshotMeta(1, 1, conf);
        RaftSnapshot snapshot = snapshot(metadata);
        raft.apply(installSnapshot(node4, 2, snapshot));
        expectFollower();
        expectTerm(2);
        verify(transportChannel4).send(installSnapshotSuccessful(node1, 2, 1));
    }

    @Test
    public void testCandidateInstallSnapshotIfTermIsNewer() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        expectTerm(2);
        ClusterConfiguration conf = configStable(node1);
        RaftSnapshotMetadata metadata = snapshotMeta(1, 1, conf);
        RaftSnapshot snapshot = snapshot(metadata);
        raft.apply(installSnapshot(node4, 3, snapshot));
        expectFollower();
        expectTerm(3);
        verify(transportChannel4).send(installSnapshotSuccessful(node1, 3, 1));
    }

    @Test
    public void testCandidateRejectInstallSnapshotIfTermIsOld() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        expectTerm(2);
        ClusterConfiguration conf = configStable(node1);
        RaftSnapshotMetadata metadata = snapshotMeta(1, 1, conf);
        RaftSnapshot snapshot = snapshot(metadata);
        raft.apply(installSnapshot(node4, 1, snapshot));
        expectCandidate();
        expectTerm(2);
        verify(transportChannel4).send(installSnapshotRejected(node1, 2));
    }

    @Test
    public void testCandidateIgnoreAddServerResponse() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        expectTerm(2);
        raft.apply(addServerResponse(AddServerResponse.Status.OK));
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
        raft.apply(removeServerResponse(RemoveServerResponse.Status.OK));
        expectCandidate();
        expectTerm(2);
    }

    // leader

    private void becameLeader() throws Exception {
        appendClusterConf();
        start();
        electionTimeout();
        expectCandidate();
        verify(transportChannel2).send(requestVote(node1, 2, 1, 1));
        verify(transportChannel3).send(requestVote(node1, 2, 1, 1));
        applyVoteCandidate(node2, 2);
        applyVoteCandidate(node3, 2);
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
            ImmutableList.of(stable(1, 1, node1, node2, node3), noopEntry(2, 2, node1)),
            raft.replicatedLog().entries()
        );
    }

    @Test
    public void testLeaderSendHeartbeatOnElection() throws Exception {
        becameLeader();
        verify(transportChannel2).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        verify(transportChannel3).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
    }

    @Test
    public void testLeaderSendHeartbeatOnSendHeartbeat() throws Exception {
        becameLeader();
        verify(transportChannel2).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        verify(transportChannel3).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        raft.apply(appendSuccessful(node2, 2, 2));
        raft.apply(appendSuccessful(node3, 2, 2));

        raft.apply(clientMessage(node2, noopEntry()));
        context.runTimer(RaftContext.SEND_HEARTBEAT);

        verify(transportChannel2).send(appendEntries(node1, 2, 2, 2, 2, noopEntry(2, 3, node2)));
        verify(transportChannel3).send(appendEntries(node1, 2, 2, 2, 2, noopEntry(2, 3, node2)));
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
        raft.apply(appendRejected(node2, 3, 1));
        expectFollower();
        expectTerm(3);
    }

    @Test
    public void testLeaderIgnoreAppendRejectedIfTermIsOld() throws Exception {
        becameLeader();
        raft.apply(appendRejected(node2, 0, 1));
        expectLeader();
        expectTerm(2);
    }

    @Test
    public void testLeaderSendPreviousMessageOnAppendRejectedIfTermMatches() throws Exception {
        appendClusterConf();
        log = log.append(noopEntry(1, 2, node1)).append(noopEntry(1, 3, node1));
        start();
        electionTimeout();
        verify(transportChannel2).send(any(RequestVote.class));
        verify(transportChannel3).send(any(RequestVote.class));
        applyVoteCandidate(node2, 2);
        applyVoteCandidate(node3, 2);

        verify(transportChannel2).send(appendEntries(node1, 2, 1, 3, 0, noopEntry(2, 4, node1)));
        verify(transportChannel3).send(appendEntries(node1, 2, 1, 3, 0, noopEntry(2, 4, node1)));

        raft.apply(appendRejected(node2, 2, 4));
        raft.apply(appendRejected(node3, 2, 4));

        // entry 4 does not included because different term
        verify(transportChannel2).send(appendEntries(node1, 2, 1, 2, 0, noopEntry(1, 3, node1)));
        verify(transportChannel3).send(appendEntries(node1, 2, 1, 2, 0, noopEntry(1, 3, node1)));

        raft.apply(appendRejected(node2, 2, 4));
        raft.apply(appendRejected(node3, 2, 4));

        verify(transportChannel2).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(1, 2, node1), noopEntry(1, 3, node1)));
        verify(transportChannel3).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(1, 2, node1), noopEntry(1, 3, node1)));
    }

    @Test
    public void testLeaderStepDownOnAppendSuccessfulIfTermIsNewer() throws Exception {
        becameLeader();
        raft.apply(appendSuccessful(node2, 3, 2));
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnAppendSuccessfulIfTermIsOld() throws Exception {
        becameLeader();
        raft.apply(appendSuccessful(node2, 1, 2));
        raft.apply(appendSuccessful(node3, 1, 2));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderCommitOnAppendSuccessfulIfTermMatches() throws Exception {
        becameLeader();
        raft.apply(appendSuccessful(node2, 2, 2));
        raft.apply(appendSuccessful(node3, 2, 2));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(2, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderRejectInstallSnapshotIfTermIsOld() throws Exception {
        becameLeader();
        ClusterConfiguration conf = configStable(node1, node2, node3);
        RaftSnapshotMetadata metadata = snapshotMeta(1, 1, conf);
        raft.apply(installSnapshot(node2, 1, snapshot(metadata)));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderRejectInstallSnapshotIfTermIsEqual() throws Exception {
        becameLeader();
        ClusterConfiguration conf = configStable(node1, node2, node3);
        RaftSnapshotMetadata metadata = snapshotMeta(1, 1, conf);
        raft.apply(installSnapshot(node2, 2, snapshot(metadata)));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotIfTermIsNewer() throws Exception {
        becameLeader();
        ClusterConfiguration conf = configStable(node1, node2, node3);
        RaftSnapshotMetadata metadata = snapshotMeta(1, 1, conf);
        raft.apply(installSnapshot(node2, 3, snapshot(metadata)));
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotSuccessfulIfTermIsNewer() throws Exception {
        becameLeader();
        raft.apply(installSnapshotSuccessful(node2, 3, 1));
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderIgnoreInstallSnapshotSuccessfulIfTermIsOld() throws Exception {
        becameLeader();
        raft.apply(installSnapshotSuccessful(node2, 1, 1));
        expectLeader();
        expectTerm(2);
    }

    @Test
    public void testLeaderHandleInstallSnapshotSuccessfulIfTermMatches() throws Exception {
        becameLeader();
        raft.apply(installSnapshotSuccessful(node2, 2, 2));
        raft.apply(installSnapshotSuccessful(node3, 2, 2));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(2, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotRejectedIfTermIsNew() throws Exception {
        becameLeader();
        raft.apply(installSnapshotRejected(node2, 3));
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderIgnoreInstallSnapshotRejectedIfTermIdOld() throws Exception {
        becameLeader();
        raft.apply(installSnapshotRejected(node2, 0));
        expectLeader();
        expectTerm(2);
        Assert.assertEquals(0, raft.replicatedLog().committedIndex());
    }

    @Test
    public void testLeaderHandleInstallSnapshotRejectedIfTermMatches() throws Exception {
        appendClusterConf();
        log = log.append(noopEntry(1, 2, node1)).append(noopEntry(1, 3, node1));
        start();
        electionTimeout();
        verify(transportChannel2).send(any(RequestVote.class));
        verify(transportChannel3).send(any(RequestVote.class));
        applyVoteCandidate(node2, 2);
        applyVoteCandidate(node3, 2);
        raft.apply(installSnapshotRejected(node2, 2));
        verify(transportChannel2).send(appendEntries(node1, 2, 1, 3, 0, noopEntry(2, 4, node1)));
    }

    @Test
    public void testLeaderUnstashClientMessages() throws Exception {
        appendClusterConf();
        start();
        expectFollower();
        raft.apply(clientMessage(node2, noopEntry()));
        electionTimeout();
        Assert.assertEquals(ImmutableList.of(clientMessage(node2, noopEntry())), raft.currentStashed());
        verify(transportChannel2).send(any(RequestVote.class));
        verify(transportChannel3).send(any(RequestVote.class));
        applyVoteCandidate(node2, 2);
        applyVoteCandidate(node3, 2);
        expectLeader();
        Assert.assertEquals(ImmutableList.of(), raft.currentStashed());
    }

    @Test
    public void testLeaderSendResponseToRemoteClient() throws Exception {
        becameLeader();
        raft.apply(appendSuccessful(node2, 2, 2));
        raft.apply(appendSuccessful(node3, 2, 2));

        raft.apply(clientMessage(node2, TestFSMMessage.getDefaultInstance()));
        raft.apply(appendSuccessful(node2, 2, 3));
        raft.apply(appendSuccessful(node3, 2, 3));

        Assert.assertEquals(3, raft.replicatedLog().committedIndex());
        verify(transportChannel2).send(TestFSMMessage.getDefaultInstance());
    }

    @Test
    public void testLeaderSendResponseToLocalClient() throws Exception {
        becameLeader();
        raft.apply(appendSuccessful(node2, 2, 2));
        raft.apply(appendSuccessful(node3, 2, 2));

        raft.apply(clientMessage(node1, TestFSMMessage.getDefaultInstance()));
        raft.apply(appendSuccessful(node2, 2, 3));
        raft.apply(appendSuccessful(node3, 2, 3));

        Assert.assertEquals(3, raft.replicatedLog().committedIndex());
        verify(transportController).dispatch(TestFSMMessage.getDefaultInstance());
    }

    @Test
    public void testLeaderRejectVoteIfTermIsEqual() throws Exception {
        becameLeader();
        applyRequestVote(2, node2, 2, 2);
        expectLeader();
        expectTerm(2);
        verify(transportChannel2).send(declineCandidate(node1, 2));
    }

    @Test
    public void testLeaderVoteCandidateIfTermIsNewer() throws Exception {
        becameLeader();
        applyRequestVote(3, node2, 2, 2);
        expectFollower();
        expectTerm(3);
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
        verify(transportChannel2).send(voteCandidate(node1, 3));
    }

    @Test
    public void testLeaderCreateSnapshot() throws Exception {
        // snapshot
        RaftSnapshotMetadata meta = snapshotMeta(2, 100, configStable(node1, node2, node3));
        RaftSnapshot snapshot = snapshot(meta);
        when(registry.prepareSnapshot(meta)).thenReturn(snapshot);

        becameLeader();
        for (int i = 0; i < 100; i++) {
            raft.apply(clientMessage(node1, noopEntry()));
        }

        verify(transportChannel2).send(appendEntries(node1, 2, 1, 1, 0, logEntry(noopEntry(), 2, 2, node1)));
        verify(transportChannel3).send(appendEntries(node1, 2, 1, 1, 0, logEntry(noopEntry(), 2, 2, node1)));
        // commit index 1
        applyAppendSuccessful(node2, 2, 100);
        applyAppendSuccessful(node3, 2, 100);

        ReplicatedLog log = raft.replicatedLog();
        logger.info("log: {}", log);
        Assert.assertTrue(log.hasSnapshot());
        Assert.assertEquals(meta, log.snapshot().getMeta());

        // send install snapshot
        raft.apply(appendRejected(node3, 2, 1));
        verify(transportChannel3).send(installSnapshot(node1, 2, snapshot));
    }

    // joint consensus

    @Test
    public void testAddServer() throws Exception {
        becameLeader();
        verify(transportChannel2).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        verify(transportChannel3).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        applyAppendSuccessful(node2, 2, 2);
        applyAppendSuccessful(node3, 2, 2);

        raft.apply(addServer(node4));
        ClusterConfiguration stable = configStable(node1, node2, node3, node4);
        LogEntry stableEntry = logEntry(stable, 2, 3, node4);
        verify(transportChannel2).send(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportChannel3).send(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        applyAppendSuccessful(node2, 2, 3);
        applyAppendSuccessful(node3, 2, 3);
        Assert.assertEquals(stable, raft.currentMeta().getConfig());
    }

    @Test
    public void testAddServerInTransitioningState() throws Exception {
        becameLeader();
        verify(transportChannel2).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        verify(transportChannel3).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        applyAppendSuccessful(node2, 2, 2);
        applyAppendSuccessful(node3, 2, 2);

        raft.apply(addServer(node4));
        ClusterConfiguration stable = configStable(node1, node2, node3, node4);
        LogEntry stableEntry = logEntry(stable, 2, 3, node4);
        verify(transportChannel2).send(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportChannel3).send(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        expectLeader();
        Assert.assertTrue(raft.currentMeta().isTransitioning());

        raft.apply(addServer(node5));
        verify(transportChannel5).send(addServerResponse(AddServerResponse.Status.TIMEOUT, node1));
    }

    @Test
    public void testRemoveServer() throws Exception {
        becameLeader();
        verify(transportChannel2).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        verify(transportChannel3).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        applyAppendSuccessful(node2, 2, 2);
        applyAppendSuccessful(node3, 2, 2);

        raft.apply(removeServer(node3));
        ClusterConfiguration stable = configStable(node1, node2);
        LogEntry stableEntry = logEntry(stable, 2, 3, node3);
        verify(transportChannel2).send(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportChannel3).send(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        applyAppendSuccessful(node2, 2, 3);
        applyAppendSuccessful(node3, 2, 3);
        Assert.assertEquals(stable, raft.currentMeta().getConfig());
    }

    @Test
    public void testRemoveServerInTransitioningState() throws Exception {
        becameLeader();
        verify(transportChannel2).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        verify(transportChannel3).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        applyAppendSuccessful(node2, 2, 2);
        applyAppendSuccessful(node3, 2, 2);

        raft.apply(removeServer(node3));
        ClusterConfiguration stable = configStable(node1, node2);
        LogEntry stableEntry = logEntry(stable, 2, 3, node3);
        verify(transportChannel2).send(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportChannel3).send(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        expectLeader();
        Assert.assertTrue(raft.currentMeta().isTransitioning());

        raft.apply(removeServer(node2));
        verify(transportChannel2).send(removeServerResponse(RemoveServerResponse.Status.TIMEOUT, node1));
    }

    @Test
    public void testLeaderRemoveSelf() throws Exception {
        becameLeader();
        verify(transportChannel2).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        verify(transportChannel3).send(appendEntries(node1, 2, 1, 1, 0, noopEntry(2, 2, node1)));
        applyAppendSuccessful(node2, 2, 2);
        applyAppendSuccessful(node3, 2, 2);

        raft.apply(removeServer(node1));
        ClusterConfiguration stable = configStable(node2, node3);
        LogEntry stableEntry = logEntry(stable, 2, 3, node1);
        verify(transportChannel2).send(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        verify(transportChannel3).send(appendEntries(node1, 2, 2, 2, 2, stableEntry));
        applyAppendSuccessful(node2, 2, 3);
        applyAppendSuccessful(node3, 2, 3);
        Assert.assertEquals(stable, raft.currentMeta().getConfig());
        expectFollower();
    }

    @Test
    public void testFollowerHandleAddServerResponse() throws Exception {
        log = log.append(logEntry(noopEntry(), 1, 1, node1)).commit(1);
        start();
        expectFollower();
        raft.apply(addServerResponse(AddServerResponse.Status.OK, node2));
        Assert.assertEquals(node2, raft.recentLeader().orElse(null));
    }

    @Test
    public void testFollowerHandleRemoveServerResponse() throws Exception {
        log = log.append(logEntry(noopEntry(), 1, 1, node1)).commit(1);
        start();
        expectFollower();
        raft.apply(removeServerResponse(RemoveServerResponse.Status.OK, node2));
        Assert.assertEquals(node2, raft.recentLeader().orElse(null));
    }

    // additional methods

    private void applyRequestVote(long term, DiscoveryNode node, long lastLogTerm, long lastLogIndex) throws IOException {
        raft.apply(requestVote(node, term, lastLogTerm, lastLogIndex));
    }

    private void applyVoteCandidate(DiscoveryNode node, long term) throws IOException {
        raft.apply(voteCandidate(node, term));
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

    private LogEntry noopEntry(long term, long index, DiscoveryNode client) {
        return logEntry(noopEntry(), term, index, client);
    }

    private LogEntry stable(long term, long index, DiscoveryNode... nodes) {
        return logEntry(configStable(nodes), term, index, node1);
    }

    private void applyAppendSuccessful(DiscoveryNode node, long term, long index) throws IOException {
        raft.apply(appendSuccessful(node, term, index));
    }

    // test dependencies

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
            Preconditions.checkNotNull(protoService);
            Preconditions.checkNotNull(clusterDiscovery);
            Preconditions.checkNotNull(registry);
            Preconditions.checkNotNull(context);
            bind(TransportService.class).toInstance(transportService);
            bind(TransportController.class).toInstance(transportController);
            bind(ProtoService.class).toInstance(protoService);
            bind(ClusterDiscovery.class).toInstance(clusterDiscovery);
            bind(ResourceRegistry.class).toInstance(registry);
            bind(RaftContext.class).toInstance(context);
            bind(FilePersistentService.class).asEagerSingleton();
            bind(PersistentService.class).to(FilePersistentService.class);
        }
    }
}
