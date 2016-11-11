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
import org.mitallast.queue.transport.TransportServer;
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
    private TransportServer transportServer;

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
        when(transportServer.localNode()).thenReturn(node1);
        when(clusterDiscovery.getDiscoveryNodes()).thenReturn(ImmutableSet.of());
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
            injector.getInstance(TransportServer.class),
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
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Init, raft.currentState());
    }

    @Test
    public void testInitKeepStateWithout1Member() throws Exception {
        start();
        raft.receive(ElectionTimeout.INSTANCE);
        raft.receive(new RaftMemberAdded(node2, 3));
        context.executeAll();
        Assert.assertEquals(Init, raft.currentState());
    }

    @Test
    public void testInitKeepStateOnMemberRemoved() throws Exception {
        start();
        raft.receive(ElectionTimeout.INSTANCE);
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberRemoved(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        raft.receive(new RaftMemberRemoved(node3, 3));
        context.executeAll();
        Assert.assertEquals(Init, raft.currentState());
    }

    @Test
    public void testInitGoToFollowerWith3Members() throws Exception {
        start();
        raft.receive(ElectionTimeout.INSTANCE);
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        context.executeAll();
        Assert.assertEquals(Follower, raft.currentState());
        Assert.assertEquals(ImmutableSet.of(node1, node2, node3), raft.currentMeta().getConfig().members());
    }

    @Test
    public void testInitStashClientMessage() throws Exception {
        start();
        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        context.executeAll();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
    }

    // follower

    @Test
    public void testStartAsFollower() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        context.executeAll();
        Assert.assertEquals(Follower, raft.currentState());
        Assert.assertEquals(log.entries(), raft.currentLog().entries());
    }

    @Test
    public void testFollowerStashClientMessage() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        context.executeAll();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
    }

    @Test
    public void testFollowerElectionTimeout() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
    }

    @Test
    public void testFollowerRejectVoteIfLastLogTermIsOld() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        context.executeAll();
        raft.receive(new RequestVote(new Term(1), node2, new Term(0), 0));
        context.executeAll();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerRejectVoteIfLastLogIndexIsOld() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        context.executeAll();
        raft.receive(new RequestVote(new Term(1), node2, new Term(1), 0));
        context.executeAll();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerRejectVoteIfAlreadyVoted() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        context.executeAll();
        raft.receive(new RequestVote(new Term(1), node2, new Term(1), 1));
        raft.receive(new RequestVote(new Term(1), node3, new Term(1), 1));
        context.executeAll();
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testFollowerRejectVoteIfTermOld() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        context.executeAll();
        raft.receive(new RequestVote(new Term(0), node2, new Term(1), 1));
        context.executeAll();
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testFollowerAcceptVoteIfTermIsNewer() throws Exception {
        log = log.append(new LogEntry(Noop.INSTANCE, new Term(1), 1)).commit(1);
        start();
        context.executeAll();
        raft.receive(new RequestVote(new Term(2), node2, new Term(1), 1));
        context.executeAll();
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
        Assert.assertEquals(new Term(2), raft.currentMeta().getCurrentTerm());
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndTermNotMatch() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        context.executeAll();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, new Term(1), 1);
        raft.receive(new AppendEntries(node2, new Term(1), new Term(1), 0, ImmutableList.of(logEntry), 0));
        context.executeAll();
        Assert.assertTrue(raft.currentLog().entries().isEmpty());
    }

    @Test
    public void testFollowerRejectAppendEntriesIfLogEmptyAndIndexNotMatch() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        context.executeAll();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, new Term(1), 1);
        raft.receive(new AppendEntries(node2, new Term(1), new Term(0), 1, ImmutableList.of(logEntry), 0));
        context.executeAll();
        Assert.assertTrue(raft.currentLog().entries().isEmpty());
    }

    @Test
    public void testFollowerAppendEntriesIfLogEmpty() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        context.executeAll();
        LogEntry logEntry = new LogEntry(Noop.INSTANCE, new Term(1), 1);
        raft.receive(new AppendEntries(node2, new Term(1), new Term(0), 0, ImmutableList.of(logEntry), 0));
        context.executeAll();
        Assert.assertEquals(ImmutableList.of(logEntry), raft.currentLog().entries());
    }

    // candidate

    @Test
    public void testCandidateRejectVoteIfLastLogTermIsOld() throws Exception {
        appendClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new RequestVote(new Term(1), node2, new Term(0), 0));
        context.executeAll();
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfTermEqual() throws Exception {
        appendClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new RequestVote(new Term(2), node2, new Term(1), 1));
        context.executeAll();
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfLastLogIndexIsOld() throws Exception {
        appendClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new RequestVote(new Term(3), node2, new Term(1), 0));
        context.executeAll();
        Assert.assertEquals(Follower, raft.currentState());
        Assert.assertFalse(raft.currentMeta().getVotedFor().isPresent());
    }

    @Test
    public void testCandidateRejectVoteIfAlreadyVoted() throws Exception {
        appendClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new RequestVote(new Term(3), node2, new Term(1), 1));
        context.executeAll();
        Assert.assertEquals(Follower, raft.currentState());
        raft.receive(new RequestVote(new Term(3), node3, new Term(1), 1));
        context.executeAll();
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateRejectVoteIfTermOld() throws Exception {
        appendClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new RequestVote(new Term(0), node2, new Term(1), 1));
        context.executeAll();
        Assert.assertEquals(node1, raft.currentMeta().getVotedFor().orElse(null));
    }

    @Test
    public void testCandidateVoteIfTermIsNewer() throws Exception {
        appendClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new RequestVote(new Term(3), node2, new Term(1), 1));
        context.executeAll();
        Assert.assertEquals(node2, raft.currentMeta().getVotedFor().orElse(null));
        Assert.assertEquals(new Term(3), raft.currentMeta().getCurrentTerm());
    }

    @Test
    public void testCandidateVoteRejectIfTermOld() throws Exception {
        appendClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new VoteCandidate(node2, new Term(1)));
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
    }

    @Test
    public void testCandidateVoteRejectIfTermNewer() throws Exception {
        appendClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new VoteCandidate(node2, new Term(3)));
        context.executeAll();
        Assert.assertEquals(Follower, raft.currentState());
        Assert.assertEquals(new Term(3), raft.currentMeta().getCurrentTerm());
    }

    @Test
    public void testCandidateVotedHasNotMajority() throws Exception {
        appendBigClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new VoteCandidate(node2, new Term(2)));
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        Assert.assertFalse(raft.currentMeta().hasMajority());
    }

    @Test
    public void testCandidateVotedHasMajority() throws Exception {
        appendBigClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new VoteCandidate(node2, new Term(2)));
        raft.receive(new VoteCandidate(node3, new Term(2)));
        context.executeAll();
        Assert.assertEquals(Leader, raft.currentState());
        Assert.assertFalse(raft.currentMeta().hasMajority());
    }

    @Test
    public void testCandidateDeclineCandidateWithTermNewer() throws Exception {
        appendBigClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new DeclineCandidate(node2, new Term(3)));
        context.executeAll();
        Assert.assertEquals(Follower, raft.currentState());
        Assert.assertEquals(new Term(3), raft.currentMeta().getCurrentTerm());
    }

    @Test
    public void testCandidateDeclineCandidateWithEqualTerm() throws Exception {
        appendBigClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new DeclineCandidate(node2, new Term(2)));
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
    }

    @Test
    public void testCandidateDeclineCandidateWithTermOld() throws Exception {
        appendBigClusterConf();
        start();
        context.executeAll();
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        raft.receive(new DeclineCandidate(node2, new Term(1)));
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
    }

    @Test
    public void testCandidateElectionTimeout() throws Exception {
        appendBigClusterConf();
        start();
        context.executeAll();
        Assert.assertEquals(Follower, raft.currentState());
        Assert.assertEquals(new Term(1), raft.currentMeta().getCurrentTerm());
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        Assert.assertEquals(new Term(2), raft.currentMeta().getCurrentTerm());
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Candidate, raft.currentState());
        Assert.assertEquals(new Term(3), raft.currentMeta().getCurrentTerm());
    }

    @Test
    public void testCandidateStashClientMessage() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        context.executeAll();
        Assert.assertEquals(ImmutableList.of(new ClientMessage(node2, Noop.INSTANCE)), raft.currentStashed());
    }

    @Test
    public void testCandidateBecameLeaderOnVoteMajority() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        raft.receive(new VoteCandidate(node2, new Term(1)));
        context.executeAll();
        Assert.assertEquals(Leader, raft.currentState());
    }

    // leader

    @Test
    public void testLeaderIgnoreElectionMessage() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        raft.receive(new VoteCandidate(node2, new Term(1)));
        raft.receive(new VoteCandidate(node3, new Term(1)));
        context.executeAll();
        raft.receive(BeginElection.INSTANCE);
        context.executeAll();
        Assert.assertEquals(Leader, raft.currentState());
    }

    @Test
    public void testLeaderAppendStableClusterConfigurationOnElection() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        raft.receive(new VoteCandidate(node2, new Term(1)));
        raft.receive(new VoteCandidate(node3, new Term(1)));
        context.executeAll();
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
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        verify(transportChannel2).message(any());
        verify(transportChannel3).message(any());

        raft.receive(new VoteCandidate(node2, new Term(1)));
        raft.receive(new VoteCandidate(node3, new Term(1)));
        context.executeAll();
        verify(transportChannel2).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(), 0));
        verify(transportChannel3).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(), 0));
    }

    @Test
    public void testLeaderSendHeartbeatOnSendHeartbeat() throws Exception {
        start();
        raft.receive(new RaftMemberAdded(node2, 3));
        raft.receive(new RaftMemberAdded(node3, 3));
        raft.receive(ElectionTimeout.INSTANCE);
        context.executeAll();
        verify(transportChannel2).message(any(RequestVote.class));
        verify(transportChannel3).message(any(RequestVote.class));

        raft.receive(new VoteCandidate(node2, new Term(1)));
        raft.receive(new VoteCandidate(node3, new Term(1)));
        context.executeAll();
        verify(transportChannel2).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(), 0));
        verify(transportChannel3).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(), 0));

        raft.receive(new ClientMessage(node2, Noop.INSTANCE));
        raft.receive(SendHeartbeat.INSTANCE);
        context.executeAll();

        LogEntry entry = new LogEntry(Noop.INSTANCE, new Term(1), 2, Optional.of(node2));
        verify(transportChannel2).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(entry), 0));
        verify(transportChannel3).message(new AppendEntries(node1, new Term(1), new Term(1), 1, ImmutableList.of(entry), 0));
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
            Preconditions.checkNotNull(transportServer);
            Preconditions.checkNotNull(clusterDiscovery);
            Preconditions.checkNotNull(resourceFSM);
            Preconditions.checkNotNull(context);
            bind(TransportService.class).toInstance(transportService);
            bind(TransportServer.class).toInstance(transportServer);
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
