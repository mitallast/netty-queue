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
import org.mitallast.queue.raft.cluster.*;
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
        when(transportService.channel(node4)).thenReturn(transportChannel3);
        when(transportService.channel(node5)).thenReturn(transportChannel3);

        context = new TestRaftContext();
        Config config = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
            .put("node.name", "test")
            .put("node.path", testFolder.getRoot().getAbsolutePath())
            .put("raft.enabled", true)
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
        log = log.append(new LogEntry(new StableClusterConfiguration(1, ImmutableSet.of(node1, node2, node3)), new Term(1), 1)).commit(1);
    }

    private void appendBigClusterConf() {
        log = log.append(new LogEntry(new StableClusterConfiguration(1, ImmutableSet.of(node1, node2, node3, node4, node5)), new Term(1), 1)).commit(1);
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
    public void testInitKeepStateWithout2Members() throws Exception {
        start();
        electionTimeout();
        execute();
        Assert.assertEquals(Init, raft.currentState());
    }

    @Test
    public void testInitKeepStateWithout1Member() throws Exception {
        start();
        electionTimeout();
        raft.receive(new RaftMemberAdded(node2, 3));
        execute();
        Assert.assertEquals(Init, raft.currentState());
    }

    @Test
    public void testInitKeepStateOnMemberRemoved() throws Exception {
        start();
        electionTimeout();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberRemoved(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        raft.receive(new RaftMemberRemoved(node3, 3));
        execute();
        Assert.assertEquals(Init, raft.currentState());
    }

    @Test
    public void testInitGoToFollowerWith3Members() throws Exception {
        start();
        electionTimeout();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        execute();
        expectFollower();
        Assert.assertEquals(ImmutableSet.of(node1, node2, node3), raft.currentMeta().getConfig().members());
    }

    @Test
    public void testInitStashClientMessage() throws Exception {
        start();
        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        execute();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
    }

    // follower

    @Test
    public void testStartAsFollower() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        execute();
        expectFollower();
        Assert.assertEquals(log.entries(), raft.currentLog().entries());
    }

    @Test
    public void testFollowerStashClientMessage() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        execute();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
    }

    @Test
    public void testFollowerElectionTimeout() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        expectCandidate();
    }

    @Test
    public void testFollowerRejectVoteIfLastLogTermIsOld() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        execute();
        requestVote(1, node2, 0, 0);
        execute();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerRejectVoteIfLastLogIndexIsOld() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        execute();
        requestVote(1, node2, 1, 0);
        execute();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerRejectVoteIfAlreadyVoted() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        execute();
        requestVote(1, node2, 1, 1);
        requestVote(1, node3, 1, 1);
        execute();
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testFollowerRejectVoteIfTermOld() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        execute();
        requestVote(0, node2, 1, 1);
        execute();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }



    @Test
    public void testFollowerAcceptVoteIfTermIsNewer() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        execute();
        requestVote(2, node2, 1, 1);
        execute();
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
        expectTerm(2);
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndTermNotMatch() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        execute();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, new Term(1), 1);
        raft.receive(new AppendEntries(node2, new Term(1), new Term(1), 0, ImmutableList.of(logEntry), 0));
        execute();
        Assert.assertTrue(raft.currentLog().entries().isEmpty());
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndIndexNotMatch() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        execute();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, new Term(1), 1);
        raft.receive(new AppendEntries(node2, new Term(1), new Term(0), 1, ImmutableList.of(logEntry), 0));
        execute();
        Assert.assertTrue(raft.currentLog().entries().isEmpty());
    }

    @Test
    public void testFollowerAppendEntriesIfLogEmpty() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        execute();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, new Term(1), 1);
        raft.receive(new AppendEntries(node2, new Term(1), new Term(0), 0, ImmutableList.of(logEntry), 0));
        execute();
        Assert.assertEquals(ImmutableList.of(logEntry), raft.currentLog().entries());
    }

    // candidate

    @Test
    public void testCandidateStepDownWithoutMembers() throws Exception {
        start();
        execute();
        raft.receive(new ChangeConfiguration(new StableClusterConfiguration(0, ImmutableSet.of(node1))));
        execute();
        raft.receive(new RaftMemberAdded(node1, 1));
        execute();
        expectFollower();
        electionTimeout();
        context.executeNext();
        expectCandidate();
        context.executeNext();
        expectFollower();
    }

    @Test
    public void testCandidateRejectVoteIfLastLogTermIsOld() throws Exception {
        appendClusterConf();
        start();
        execute();
        electionTimeout();
        execute();
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
        execute();
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
        execute();
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
        execute();
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
        execute();
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
        execute();
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
        execute();
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
        execute();
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
        execute();
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
        execute();
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
        execute();
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
        execute();
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
        execute();
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
        execute();
        expectCandidate();
        expectTerm(2);
        electionTimeout();
        execute();
        expectCandidate();
        expectTerm(3);
    }

    @Test
    public void testCandidateStashClientMessage() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        execute();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
    }

    @Test
    public void testCandidateBecameLeaderOnVoteMajority() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        execute();
        expectLeader();
    }

    @Test
    public void testCandidateIgnoreAppendEntriesIfTermIsOld() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        raft.receive(new AppendEntries(node2, new Term(0), new Term(0), 0, ImmutableList.of(), 0));
        execute();
        expectTerm(1);
        expectCandidate();
    }

    @Test
    public void testCandidateIgnoreAppendEntriesIfTermIsEqual() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        LogEntry entry = new LogEntry(Noop.INSTANCE, new Term(1), 1);
        raft.receive(new AppendEntries(node2, new Term(1), new Term(0), 0, ImmutableList.of(entry), 0));
        execute();
        execute();
        expectTerm(1);
        expectFollower();
        Assert.assertEquals(ImmutableList.of(entry), raft.currentLog().entries());
    }

    // leader

    @Test
    public void testLeaderIgnoreElectionMessage() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(BeginElection.INSTANCE);
        execute();
        expectLeader();
    }

    @Test
    public void testLeaderAppendStableClusterConfigurationOnElection() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        Assert.assertEquals(
            ImmutableList.of(new LogEntry(
                new StableClusterConfiguration(0, ImmutableSet.of(node1, node2, node3)),
                new Term(1),
                1,
                Optional.empty()
            )),
            raft.currentLog().entries()
        );
    }

    @Test
    public void testLeaderSendHeartbeatOnElection() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        verify(transportChannel2).message(any());
        verify(transportChannel3).message(any());

        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        verify(transportChannel2).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(), 0));
        verify(transportChannel3).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(), 0));
    }

    @Test
    public void testLeaderSendHeartbeatOnSendHeartbeat() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));

        voteCandidate(node2, 1);
        voteCandidate(node3, 1);

        execute();
        verify(transportChannel2).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(), 0));
        verify(transportChannel3).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(), 0));

        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        raft.receive(SendHeartbeat.INSTANCE);
        execute();

        LogEntry entry = new LogEntry(Noop.INSTANCE, new Term(1), 2, Optional.of(node2));
        verify(transportChannel2).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(entry), 0));
        verify(transportChannel3).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(entry), 0));
    }

    @Test
    public void testLeaderRejectAppendEntriesIfTermIsLower() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new AppendEntries(node2, new Term(0), new Term(0), 0, ImmutableList.of(), 0));
        execute();
        expectLeader();
        expectTerm(1);
    }

    @Test
    public void testLeaderRejectAppendEntriesIfTermIsEqual() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new AppendEntries(node2, new Term(1), new Term(0), 0, ImmutableList.of(), 0));
        execute();
        expectLeader();
        expectTerm(1);
    }

    @Test
    public void testLeaderStepDownOnAppendEntriesIfTermIsNewer() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new AppendEntries(node2, new Term(2), new Term(0), 0, ImmutableList.of(), 0));
        execute();
        expectFollower();
        expectTerm(2);
    }

    @Test
    public void testLeaderStepDownOnAppendRejectedIfTermIsNewer() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new AppendRejected(node2, new Term(2)));
        execute();
        expectFollower();
        expectTerm(2);
    }

    @Test
    public void testLeaderIgnoreAppendRejectedIfTermIsOld() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new AppendRejected(node2, new Term(0)));
        execute();
        expectLeader();
        expectTerm(1);
    }

    @Test
    public void testLeaderSendPreviousMessageOnAppendRejectedIfTermMatches() throws Exception {
        appendClusterConf();
        LogEntry entry2 = new LogEntry(Noop.INSTANCE, new Term(1), 2);
        LogEntry entry3 = new LogEntry(Noop.INSTANCE, new Term(1), 3);
        LogEntry entry4 = new LogEntry(Noop.INSTANCE, new Term(2), 4);

        log = log.append(entry2).append(entry3);
        start();
        execute();
        electionTimeout();
        execute();
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        execute();

        verify(transportChannel2).message(new AppendEntries(node1, new Term(2), new Term(1), 3, ImmutableList.of(entry4), 1));
        verify(transportChannel3).message(new AppendEntries(node1, new Term(2), new Term(1), 3, ImmutableList.of(entry4), 1));

        raft.receive(new AppendRejected(node2, new Term(2)));
        raft.receive(new AppendRejected(node3, new Term(2)));
        execute();

        // entry 4 does not included because different term
        verify(transportChannel2).message(new AppendEntries(node1, new Term(2), new Term(1), 2, ImmutableList.of(entry3), 1));
        verify(transportChannel3).message(new AppendEntries(node1, new Term(2), new Term(1), 2, ImmutableList.of(entry3), 1));

        raft.receive(new AppendRejected(node2, new Term(2)));
        raft.receive(new AppendRejected(node3, new Term(2)));
        execute();

        verify(transportChannel2).message(new AppendEntries(node1, new Term(2), new Term(1), 1, ImmutableList.of(entry2, entry3), 1));
        verify(transportChannel3).message(new AppendEntries(node1, new Term(2), new Term(1), 1, ImmutableList.of(entry2, entry3), 1));
    }

    @Test
    public void testLeaderStepDownOnAppendSuccessfullIfTermIsNewer() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new AppendSuccessful(node2, new Term(2), 1));
        raft.receive(new AppendSuccessful(node3, new Term(2), 1));
        execute();
        expectFollower();
        expectTerm(2);
        Assert.assertEquals(0, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnAppendSuccessfullIfTermIsOld() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new AppendSuccessful(node2, new Term(0), 1));
        raft.receive(new AppendSuccessful(node3, new Term(0), 1));
        execute();
        expectLeader();
        expectTerm(1);
        Assert.assertEquals(0, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderCommitOnAppendSuccessfullIfTermMatches() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new AppendSuccessful(node2, new Term(1), 1));
        raft.receive(new AppendSuccessful(node3, new Term(1), 1));
        execute();
        expectLeader();
        expectTerm(1);
        Assert.assertEquals(1, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderRejectInstallSnapshotIfTermIsOld() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        StableClusterConfiguration conf = new StableClusterConfiguration(1, ImmutableSet.of(node1, node2, node3));
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(new Term(1), 1, conf);
        raft.receive(new InstallSnapshot(node2, new Term(0), new RaftSnapshot(metadata, Noop.INSTANCE)));
        execute();
        expectLeader();
        expectTerm(1);
        Assert.assertEquals(0, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderRejectInstallSnapshotIfTermIsEqual() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        StableClusterConfiguration conf = new StableClusterConfiguration(1, ImmutableSet.of(node1, node2, node3));
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(new Term(1), 1, conf);
        raft.receive(new InstallSnapshot(node2, new Term(1), new RaftSnapshot(metadata, Noop.INSTANCE)));
        execute();
        expectLeader();
        expectTerm(1);
        Assert.assertEquals(0, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotIfTermIsNewer() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        StableClusterConfiguration conf = new StableClusterConfiguration(1, ImmutableSet.of(node1, node2, node3));
        RaftSnapshotMetadata metadata = new RaftSnapshotMetadata(new Term(1), 1, conf);
        raft.receive(new InstallSnapshot(node2, new Term(2), new RaftSnapshot(metadata, Noop.INSTANCE)));
        execute();
        expectFollower();
        expectTerm(2);
        Assert.assertEquals(0, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotSuccessfulIfTermIsNewer() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new InstallSnapshotSuccessful(node2, new Term(2), 1));
        execute();
        expectFollower();
        expectTerm(2);
        Assert.assertEquals(0, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderIgnoreInstallSnapshotSuccessfullIfTermIsOld() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new InstallSnapshotSuccessful(node2, new Term(0), 1));
        execute();
        expectLeader();
        expectTerm(1);
    }

    @Test
    public void testLeaderHandleInstallSnapshotSuccessfullIfTermMatches() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new InstallSnapshotSuccessful(node2, new Term(1), 1));
        raft.receive(new InstallSnapshotSuccessful(node3, new Term(1), 1));
        execute();
        expectLeader();
        expectTerm(1);
        Assert.assertEquals(1, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderStepDownOnInstallSnapshotRejectedIfTermIsNew() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new InstallSnapshotRejected(node2, new Term(2)));
        execute();
        expectFollower();
        expectTerm(2);
        Assert.assertEquals(0, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderIgnoreInstallSnapshotRejectedIfTermIdOld() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new InstallSnapshotRejected(node2, new Term(0)));
        execute();
        expectLeader();
        expectTerm(1);
        Assert.assertEquals(0, raft.currentLog().committedIndex());
    }

    @Test
    public void testLeaderHandleInstallSnapshotRejectedIfTermMatches() throws Exception {
        appendClusterConf();
        LogEntry entry2 = new LogEntry(Noop.INSTANCE, new Term(1), 2);
        LogEntry entry3 = new LogEntry(Noop.INSTANCE, new Term(1), 3);
        LogEntry entry4 = new LogEntry(Noop.INSTANCE, new Term(2), 4);
        log = log.append(entry2).append(entry3).commit(3);
        start();
        execute();
        electionTimeout();
        execute();
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));
        voteCandidate(node2, 2);
        voteCandidate(node3, 2);
        execute();
        raft.receive(new InstallSnapshotRejected(node2, new Term(2)));
        execute();
        verify(transportChannel2).message(new AppendEntries(node1, new Term(2), new Term(1), 3, ImmutableList.of(entry4), 3));
    }

    @Test
    public void testLeaderHandleRequestConfiguration() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        electionTimeout();
        execute();
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        raft.receive(new RequestConfiguration(node2));
        execute();
        verify(transportChannel2).message(any(AppendEntries.class));
        verify(transportChannel3).message(any(AppendEntries.class));
        verify(transportChannel2).message(new ChangeConfiguration(new StableClusterConfiguration(0, ImmutableSet.of(node1, node2, node3))));
    }

    @Test
    public void testLeaderUnstashClientMessages() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        execute();
        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        electionTimeout();
        execute();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));
        voteCandidate(node2, 1);
        voteCandidate(node3, 1);
        execute();
        Assert.assertEquals(ImmutableList.of(), raft.currentStashed());
    }

    private void requestVote(long term, DiscoveryNode node, long lastLogTerm, long lastLogindex) {
        raft.receive(new RequestVote(new Term(term), node, new Term(lastLogTerm), lastLogindex));
    }

    private void voteCandidate(DiscoveryNode node, long term) {
        raft.receive(new VoteCandidate(node, new Term(term)));
    }

    private void execute() {
        context.executeAll();
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
    }

    private class TestRaftContext implements RaftContext {

        private LinkedList<Runnable> queue = new LinkedList<>();

        @Override
        public void execute(Runnable runnable) {
            queue.add(runnable);
        }

        public void executeNext() {
            queue.pop().run();
        }

        public void executeAll() {
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

            streamableBinder.addBinding().toInstance(StreamableRegistry.of(JointConsensusClusterConfiguration.class, JointConsensusClusterConfiguration::new, 100));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftMemberAdded.class, RaftMemberAdded::new, 101));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftMemberRemoved.class, RaftMemberRemoved::new, 102));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftMembersDiscoveryRequest.class, RaftMembersDiscoveryRequest::new, 103));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftMembersDiscoveryResponse.class, RaftMembersDiscoveryResponse::new, 104));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftMembersDiscoveryTimeout.class, RaftMembersDiscoveryTimeout::read, 105));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(StableClusterConfiguration.class, StableClusterConfiguration::new, 106));


            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendEntries.class, AppendEntries::new, 200));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendRejected.class, AppendRejected::new, 201));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendSuccessful.class, AppendSuccessful::new, 202));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(BeginElection.class, BeginElection::read, 204));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(ChangeConfiguration.class, ChangeConfiguration::new, 205));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(ClientMessage.class, ClientMessage::new, 206));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(DeclineCandidate.class, DeclineCandidate::new, 207));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(ElectedAsLeader.class, ElectedAsLeader::read, 208));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(ElectionTimeout.class, ElectionTimeout::read, 209));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(InitLogSnapshot.class, InitLogSnapshot::read, 211));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshot.class, InstallSnapshot::new, 212));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshotRejected.class, InstallSnapshotRejected::new, 213));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshotSuccessful.class, InstallSnapshotSuccessful::new, 214));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(LogEntry.class, LogEntry::new, 216));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftSnapshot.class, RaftSnapshot::new, 217));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftSnapshotMetadata.class, RaftSnapshotMetadata::new, 218));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RequestConfiguration.class, RequestConfiguration::new, 219));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(RequestVote.class, RequestVote::new, 220));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(SendHeartbeat.class, SendHeartbeat::read, 221));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(VoteCandidate.class, VoteCandidate::new, 222));
            streamableBinder.addBinding().toInstance(StreamableRegistry.of(Noop.class, Noop::read, 223));
        }
    }
}
