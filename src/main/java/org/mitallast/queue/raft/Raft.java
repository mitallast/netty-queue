package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.logging.log4j.CloseableThreadContext;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.Match;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.events.EventBus;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.event.ServerAdded;
import org.mitallast.queue.raft.event.ServerRemoved;
import org.mitallast.queue.raft.persistent.PersistentService;
import org.mitallast.queue.raft.persistent.ReplicatedLog;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.raft.resource.ResourceRegistry;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;

import java.io.IOError;
import java.io.IOException;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.mitallast.queue.raft.RaftState.*;

public class Raft extends AbstractLifecycleComponent {
    private final TransportService transportService;
    private final TransportController transportController;
    private final ClusterDiscovery clusterDiscovery;
    private final PersistentService persistentService;
    private final ReplicatedLog replicatedLog;
    private final ResourceRegistry registry;
    private final boolean bootstrap;
    private final long electionDeadline;
    private final long heartbeat;
    private final int snapshotInterval;
    private final int maxEntries;
    private final RaftContext context;
    private final EventBus eventBus;
    private final ConcurrentLinkedQueue<ClientMessage> stashed;
    private final ReentrantLock lock;
    private volatile Optional<DiscoveryNode> recentlyContactedByLeader;
    private volatile ImmutableMap<DiscoveryNode, Long> replicationIndex;
    private volatile LogIndexMap nextIndex;
    private volatile LogIndexMap matchIndex;
    private volatile State state;

    private final Match.Mapper<Streamable, State> mapper = Match.<Streamable, State>map()
        .when(AppendEntries.class, (e) -> state.handle(e))
        .when(AppendRejected.class, (e) -> state.handle(e))
        .when(AppendSuccessful.class, (e) -> state.handle(e))
        .when(RequestVote.class, (e) -> state.handle(e))
        .when(VoteCandidate.class, (e) -> state.handle(e))
        .when(DeclineCandidate.class, (e) -> state.handle(e))
        .when(ClientMessage.class, (e) -> state.handle(e))
        .when(InstallSnapshot.class, (e) -> state.handle(e))
        .when(InstallSnapshotSuccessful.class, (e) -> state.handle(e))
        .when(InstallSnapshotRejected.class, (e) -> state.handle(e))
        .when(AddServer.class, (e) -> state.handle(e))
        .when(AddServerResponse.class, (e) -> state.handle(e))
        .when(RemoveServer.class, (e) -> state.handle(e))
        .when(RemoveServerResponse.class, (e) -> state.handle(e))
        .build();

    @Inject
    public Raft(
        Config config,
        TransportService transportService,
        TransportController transportController,
        ClusterDiscovery clusterDiscovery,
        PersistentService persistentService,
        ResourceRegistry registry,
        RaftContext context,
        EventBus eventBus
    ) throws IOException {
        this.transportService = transportService;
        this.transportController = transportController;
        this.clusterDiscovery = clusterDiscovery;
        this.persistentService = persistentService;
        this.replicatedLog = persistentService.openLog();
        this.registry = registry;
        this.context = context;
        this.eventBus = eventBus;

        stashed = new ConcurrentLinkedQueue<>();
        lock = new ReentrantLock();
        recentlyContactedByLeader = Optional.empty();
        nextIndex = new LogIndexMap(0);
        matchIndex = new LogIndexMap(0);

        bootstrap = config.getBoolean("raft.bootstrap");
        electionDeadline = config.getDuration("raft.election-deadline", TimeUnit.MILLISECONDS);
        heartbeat = config.getDuration("raft.heartbeat", TimeUnit.MILLISECONDS);
        snapshotInterval = config.getInt("raft.snapshot-interval");
        maxEntries = config.getInt("raft.max-entries");
    }

    @Override
    protected void doStart() throws IOException {
        RaftMetadata meta = new RaftMetadata(
            persistentService.currentTerm(),
            new StableClusterConfiguration(),
            persistentService.votedFor()
        );
        state = new FollowerState(meta).initialize();
    }

    @Override
    protected void doStop() {
        context.cancelTimer(RaftContext.ELECTION_TIMEOUT);
        context.cancelTimer(RaftContext.SEND_HEARTBEAT);
    }

    @Override
    protected void doClose() {
    }

    // fsm related

    public void apply(Streamable event) {
        lock.lock();
        try {
            if (state == null) {
                return;
            }
            try (final CloseableThreadContext.Instance ignored = CloseableThreadContext.push(state.state().name())) {
                state = mapper.apply(event);
            }
        } catch (IOException e) {
            logger.error("error apply event", e);
            throw new IOError(e);
        } finally {
            lock.unlock();
        }
    }

    public Optional<DiscoveryNode> recentLeader() {
        return recentlyContactedByLeader;
    }

    public RaftState currentState() {
        return state.state();
    }

    public RaftMetadata currentMeta() {
        return state.meta();
    }

    public ReplicatedLog replicatedLog() {
        return replicatedLog;
    }

    public ImmutableList<Streamable> currentStashed() {
        return ImmutableList.copyOf(stashed);
    }

    // behavior related

    private void send(DiscoveryNode node, Streamable message) {
        if (node.equals(clusterDiscovery.self())) {
            transportController.dispatch(message);
        } else {
            try {
                transportService.connectToNode(node);
                transportService.channel(node).send(message);
            } catch (IOException e) {
                logger.warn("error send message to {}", node, e);
            }
        }
    }

    private void senderIsCurrentLeader(DiscoveryNode leader) {
        logger.debug("leader is {}", leader);
        recentlyContactedByLeader = Optional.of(leader);
    }

    // additional classes

    private abstract class State {
        private RaftMetadata meta;

        private State(RaftMetadata meta) throws IOException {
            this.meta = meta;
            persistentService.updateState(meta.getCurrentTerm(), meta.getVotedFor());
            state = this;
        }

        protected State stay(RaftMetadata meta) throws IOException {
            if (!meta.getConfig().isTransitioning()) {
                if (this.meta.getConfig().isTransitioning()) {
                    JointConsensusClusterConfiguration joint = (JointConsensusClusterConfiguration) this.meta.getConfig();
                    for (DiscoveryNode node : joint.getOldMembers()) {
                        if (!meta.members().contains(node)) {
                            eventBus.trigger(new ServerRemoved(node));
                        }
                    }
                    for (DiscoveryNode node : meta.members()) {
                        if (!joint.getOldMembers().contains(node)) {
                            eventBus.trigger(new ServerAdded(node));
                        }
                    }
                } else if (this.meta.members().size() != meta.members().size()) {
                    for (DiscoveryNode node : this.meta.members()) {
                        if (!meta.members().contains(node)) {
                            eventBus.trigger(new ServerRemoved(node));
                        }
                    }
                    for (DiscoveryNode node : meta.members()) {
                        if (!this.meta.members().contains(node)) {
                            eventBus.trigger(new ServerAdded(node));
                        }
                    }
                }
            }
            this.meta = meta;
            persistentService.updateState(meta.getCurrentTerm(), meta.getVotedFor());
            return this;
        }

        public abstract RaftState state();

        public RaftMetadata meta() {
            return meta;
        }

        // replication

        public abstract State handle(AppendEntries message) throws IOException;

        public State handle(AppendRejected message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return this;
        }

        public State handle(AppendSuccessful message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return this;
        }

        // election

        public abstract State handle(RequestVote message) throws IOException;

        public State handle(VoteCandidate message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return this;
        }

        public State handle(DeclineCandidate message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return this;
        }

        // leader

        public abstract State handle(ClientMessage message) throws IOException;

        // snapshot

        public State createSnapshot() throws IOException {
            long committedIndex = replicatedLog.committedIndex();
            RaftSnapshotMetadata snapshotMeta = new RaftSnapshotMetadata(replicatedLog.termAt(committedIndex),
                committedIndex, meta().getConfig());
            logger.info("init snapshot up to: {}:{}", snapshotMeta.getLastIncludedIndex(), snapshotMeta
                .getLastIncludedTerm());

            RaftSnapshot snapshot = registry.prepareSnapshot(snapshotMeta);
            logger.info("successfully prepared snapshot for {}:{}, compacting log now", snapshotMeta
                .getLastIncludedIndex(), snapshotMeta.getLastIncludedTerm());
            replicatedLog.compactWith(snapshot, clusterDiscovery.self());

            return this;
        }

        public abstract State handle(InstallSnapshot message) throws IOException;

        public State handle(InstallSnapshotSuccessful message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return this;
        }

        public State handle(InstallSnapshotRejected message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return this;
        }

        // joint consensus

        public abstract State handle(AddServer request) throws IOException;

        public State handle(AddServerResponse message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return this;
        }

        public abstract State handle(RemoveServer request) throws IOException;

        public State handle(RemoveServerResponse message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return this;
        }

        // stash messages

        public void stash(ClientMessage streamable) throws IOException {
            logger.debug("stash {}", streamable);
            stashed.add(streamable);
        }
    }

    private class FollowerState extends State {

        public FollowerState(RaftMetadata meta) throws IOException {
            super(meta);
        }

        @Override
        public RaftState state() {
            return Follower;
        }

        protected FollowerState stay(RaftMetadata meta) throws IOException {
            super.stay(meta);
            return this;
        }

        private State gotoCandidate() throws IOException {
            resetElectionDeadline();
            return new CandidateState(this.meta().forNewElection()).beginElection();
        }

        public FollowerState resetElectionDeadline() throws IOException {
            logger.debug("reset election deadline");
            long timeout = new Random().nextInt((int) (electionDeadline / 2)) + electionDeadline;
            context.setTimer(RaftContext.ELECTION_TIMEOUT, timeout, () -> {
                lock.lock();
                try {
                    if (state == this) {
                        state = electionTimeout();
                    } else {
                        throw new IllegalStateException();
                    }
                } catch (IOException | IllegalStateException e) {
                    logger.error("error handle election timeout", e);
                } finally {
                    lock.unlock();
                }
            });
            return this;
        }

        public State initialize() throws IOException {
            resetElectionDeadline();
            if (replicatedLog.isEmpty()) {
                if (bootstrap) {
                    logger.info("bootstrap cluster");
                    return handle(new AddServer(clusterDiscovery.self()));
                } else {
                    logger.info("joint cluster");
                    return electionTimeout();
                }
            } else {
                ClusterConfiguration config = replicatedLog.entries().stream()
                    .map(LogEntry::getCommand)
                    .filter(cmd -> cmd instanceof ClusterConfiguration)
                    .map(cmd -> (ClusterConfiguration) cmd)
                    .reduce(meta().getConfig(), (a, b) -> b);

                RaftMetadata meta = meta().withConfig(config).withTerm(replicatedLog.lastTerm());
                return stay(meta);
            }
        }

        @Override
        @SuppressWarnings("ConstantConditions")
        public State handle(ClientMessage message) throws IOException {
            if (recentlyContactedByLeader.isPresent()) {
                send(recentlyContactedByLeader.get(), message);
            } else {
                stash(message);
            }
            return this;
        }

        @Override
        public State handle(RequestVote message) throws IOException {
            RaftMetadata meta = meta();
            if (message.getTerm() > meta.getCurrentTerm()) {
                logger.info("received newer {}, current term is {}", message.getTerm(), meta.getCurrentTerm());
                meta = meta.withTerm(message.getTerm());
            }
            if (meta.canVoteIn(message.getTerm())) {
                resetElectionDeadline();
                if (replicatedLog.lastTerm().filter(term -> message.getLastLogTerm() < term).isPresent()) {
                    logger.warn("rejecting vote for {} at term {}, candidate's lastLogTerm: {} < ours: {}",
                        message.getCandidate(),
                        message.getTerm(),
                        message.getLastLogTerm(),
                        replicatedLog.lastTerm());
                    send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta.getCurrentTerm()));
                    return stay(meta);
                }
                if (replicatedLog.lastTerm().filter(term -> term.equals(message.getLastLogTerm())).isPresent() &&
                    message.getLastLogIndex() < replicatedLog.lastIndex()) {
                    logger.warn("rejecting vote for {} at term {}, candidate's lastLogIndex: {} < ours: {}",
                        message.getCandidate(),
                        message.getTerm(),
                        message.getLastLogIndex(),
                        replicatedLog.lastIndex());
                    send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta.getCurrentTerm()));
                    return stay(meta);
                }

                logger.info("voting for {} in {}", message.getCandidate(), message.getTerm());
                send(message.getCandidate(), new VoteCandidate(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay(meta.withVoteFor(message.getCandidate()));
            } else if (meta.getVotedFor().isPresent()) {
                logger.warn("rejecting vote for {}, and {}, currentTerm: {}, already voted for: {}",
                    message.getCandidate(),
                    message.getTerm(),
                    meta.getCurrentTerm(),
                    meta.getVotedFor());
                send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay(meta);
            } else {
                logger.warn("rejecting vote for {}, and {}, currentTerm: {}, received stale term number {}",
                    message.getCandidate(), message.getTerm(),
                    meta.getCurrentTerm(), message.getTerm());
                send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay(meta);
            }
        }

        @Override
        public State handle(AppendEntries message) throws IOException {
            RaftMetadata meta = meta();
            if (message.getTerm() > meta.getCurrentTerm()) {
                logger.info("received newer {}, current term is {}", message.getTerm(), meta.getCurrentTerm());
                meta = meta.withTerm(message.getTerm());
            }
            // 1) Reply false if term < currentTerm (5.1)
            if (message.getTerm() < meta.getCurrentTerm()) {
                logger.warn("rejecting write (old term): {} < {} ", message.getTerm(), meta.getCurrentTerm());
                send(message.getMember(), new AppendRejected(clusterDiscovery.self(), meta.getCurrentTerm(),
                    replicatedLog.lastIndex()));
                return stay(meta);
            }

            try {
                // 2) Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (5.3)
                if (!replicatedLog.containsMatchingEntry(message.getPrevLogTerm(), message.getPrevLogIndex())) {
                    logger.warn("rejecting write (inconsistent log): {}:{} {} ",
                        message.getPrevLogTerm(), message.getPrevLogIndex(),
                        replicatedLog);
                    send(message.getMember(), new AppendRejected(clusterDiscovery.self(), meta.getCurrentTerm(),
                        replicatedLog.lastIndex()));
                    return stay(meta);
                } else {
                    return appendEntries(message, meta);
                }
            } finally {
                resetElectionDeadline();
            }
        }

        private State appendEntries(AppendEntries msg, RaftMetadata meta) throws IOException {
            senderIsCurrentLeader(msg.getMember());

            if (!msg.getEntries().isEmpty()) {
                // If an existing entry conflicts with a new one (same index
                // but different terms), delete the existing entry and all that
                // follow it (5.3)

                // Append any new entries not already in the log

                logger.debug("append({})", msg.getEntries());
                replicatedLog.append(msg.getEntries());
            }
            logger.debug("response append successful term:{} lastIndex:{}", meta.getCurrentTerm(), replicatedLog.lastIndex());
            AppendSuccessful response = new AppendSuccessful(clusterDiscovery.self(), meta.getCurrentTerm(), replicatedLog.lastIndex());
            send(msg.getMember(), response);

            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

            if (msg.getLeaderCommit() > replicatedLog.committedIndex()) {
                ImmutableList<LogEntry> entries = replicatedLog.slice(replicatedLog.committedIndex() + 1, msg.getLeaderCommit());
                for (LogEntry entry : entries) {
                    if (entry.getCommand() instanceof ClusterConfiguration) {
                        logger.info("apply new configuration: {}", entry.getCommand());
                        meta = meta.withConfig((ClusterConfiguration) entry.getCommand());
                    } else if (entry.getCommand() instanceof Noop) {
                        logger.trace("ignore noop entry");
                    } else if (entry.getCommand() instanceof RaftSnapshot) {
                        logger.warn("unexpected raft snapshot in log");
                    } else {
                        logger.debug("committing entry {} on follower, leader is committed until [{}]", entry, msg.getLeaderCommit());
                        registry.apply(entry.getIndex(), entry.getCommand());
                    }
                    replicatedLog.commit(entry.getIndex());
                }
            }

            ClusterConfiguration config = msg.getEntries().stream()
                .map(LogEntry::getCommand)
                .filter(cmd -> cmd instanceof ClusterConfiguration)
                .map(cmd -> (ClusterConfiguration) cmd)
                .reduce(meta.getConfig(), (a, b) -> b);

            FollowerState newState = stay(meta.withTerm(replicatedLog.lastTerm()).withConfig(config));
            newState.unstash();
            if (replicatedLog.committedEntries() >= snapshotInterval) {
                return newState.createSnapshot();
            } else {
                return newState;
            }
        }

        public State electionTimeout() throws IOException {
            resetElectionDeadline();
            if (meta().getConfig().members().isEmpty()) {
                logger.info("no members found, joint timeout");
                for (DiscoveryNode node : clusterDiscovery.discoveryNodes()) {
                    if (!node.equals(clusterDiscovery.self())) {
                        send(node, new AddServer(clusterDiscovery.self()));
                    }
                }
                return this;
            } else {
                return gotoCandidate();
            }
        }

        // joint consensus

        @Override
        public State handle(AddServer request) throws IOException {
            if (bootstrap && meta().getConfig().members().isEmpty() &&
                request.getMember().equals(clusterDiscovery.self()) &&
                replicatedLog.isEmpty()) {
                context.cancelTimer(RaftContext.ELECTION_TIMEOUT);
                return new LeaderState(this.meta().forLeader()).selfJoin();
            }
            send(request.getMember(), new AddServerResponse(
                AddServerResponse.Status.NOT_LEADER,
                recentlyContactedByLeader
            ));
            return this;
        }

        public State handle(AddServerResponse request) throws IOException {
            if (request.getStatus() == AddServerResponse.Status.OK) {
                logger.info("successful joined");
            }
            Optional<DiscoveryNode> leader = request.getLeader();
            if (leader.isPresent()) {
                senderIsCurrentLeader(leader.get());
                resetElectionDeadline();
            }
            return this;
        }

        @Override
        public State handle(RemoveServer request) throws IOException {
            send(request.getMember(), new RemoveServerResponse(
                RemoveServerResponse.Status.NOT_LEADER,
                recentlyContactedByLeader
            ));
            return this;
        }

        public State handle(RemoveServerResponse request) throws IOException {
            if (request.getStatus() == RemoveServerResponse.Status.OK) {
                logger.info("successful removed");
            }
            recentlyContactedByLeader = request.getLeader();
            resetElectionDeadline();
            return this;
        }

        @Override
        public State handle(InstallSnapshot message) throws IOException {
            RaftMetadata meta = meta();
            if (message.getTerm() > meta.getCurrentTerm()) {
                logger.info("received newer {}, current term is {}", message.getTerm(), meta.getCurrentTerm());
                meta = meta.withTerm(message.getTerm());
            }
            if (message.getTerm() < meta.getCurrentTerm()) {
                logger.info("rejecting install snapshot {}, current term is {}", message.getTerm(), meta
                    .getCurrentTerm());
                send(message.getLeader(), new InstallSnapshotRejected(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay(meta);
            } else {
                resetElectionDeadline();
                logger.info("got snapshot from {}, is for: {}", message.getLeader(), message.getSnapshot().getMeta());

                meta = meta.withConfig(message.getSnapshot().getMeta().getConfig());
                replicatedLog.compactWith(message.getSnapshot(), clusterDiscovery.self());
                for (Streamable streamable : message.getSnapshot().getData()) {
                    registry.apply(message.getSnapshot().getMeta().getLastIncludedIndex(), streamable);
                }

                logger.info("response snapshot installed in {} last index {}", meta.getCurrentTerm(), replicatedLog
                    .lastIndex());
                send(message.getLeader(), new InstallSnapshotSuccessful(clusterDiscovery.self(), meta.getCurrentTerm
                    (), replicatedLog.lastIndex()));

                return stay(meta);
            }
        }

        @SuppressWarnings("ConstantConditions")
        private void unstash() throws IOException {
            if (recentlyContactedByLeader.isPresent()) {
                DiscoveryNode leader = recentlyContactedByLeader.get();
                Streamable poll;
                while ((poll = stashed.poll()) != null) {
                    send(leader, poll);
                }
            } else {
                logger.warn("try unstash without leader");
            }
        }
    }

    private class CandidateState extends State {

        public CandidateState(RaftMetadata meta) throws IOException {
            super(meta);
        }

        @Override
        public RaftState state() {
            return Candidate;
        }

        @Override
        protected CandidateState stay(RaftMetadata meta) throws IOException {
            super.stay(meta);
            return this;
        }

        private FollowerState gotoFollower() throws IOException {
            return new FollowerState(this.meta().forFollower()).resetElectionDeadline();
        }

        public State gotoLeader() throws IOException {
            context.cancelTimer(RaftContext.ELECTION_TIMEOUT);
            return new LeaderState(this.meta().forLeader()).elected();
        }

        @Override
        public State handle(ClientMessage message) throws IOException {
            stash(message);
            return this;
        }

        @Override
        public State handle(AddServer request) throws IOException {
            send(request.getMember(), new AddServerResponse(
                AddServerResponse.Status.NOT_LEADER,
                Optional.empty()
            ));
            return this;
        }

        @Override
        public State handle(RemoveServer request) throws IOException {
            send(request.getMember(), new RemoveServerResponse(
                RemoveServerResponse.Status.NOT_LEADER,
                Optional.empty()
            ));
            return this;
        }

        private void resetElectionDeadline() throws IOException {
            logger.debug("reset election deadline");
            long timeout = new Random().nextInt((int) (electionDeadline / 2)) + electionDeadline;
            context.setTimer(RaftContext.ELECTION_TIMEOUT, timeout, () -> {
                lock.lock();
                try {
                    if (state == this) {
                        state = electionTimeout();
                    } else {
                        throw new IllegalStateException();
                    }
                } catch (IOException | IllegalStateException e) {
                    logger.error("error handle election timeout", e);
                } finally {
                    lock.unlock();
                }
            });
        }

        private State beginElection() throws IOException {
            resetElectionDeadline();
            RaftMetadata meta = meta();
            logger.info("initializing election (among {} nodes) for {}", meta.getConfig().members().size(), meta
                .getCurrentTerm());
            RequestVote request = new RequestVote(meta.getCurrentTerm(), clusterDiscovery.self(), replicatedLog
                .lastTerm().orElse(0L), replicatedLog.lastIndex());
            for (DiscoveryNode member : meta.membersWithout(clusterDiscovery.self())) {
                logger.info("send request vote to {}", member);
                send(member, request);
            }
            meta = meta.incVote().withVoteFor(clusterDiscovery.self());
            if (meta.hasMajority()) {
                logger.info("received vote by {}, won election with {} of {} votes", clusterDiscovery.self(), meta
                    .getVotesReceived(), meta.getConfig().members().size());
                return stay(meta).gotoLeader();
            } else {
                return stay(meta);
            }
        }

        @Override
        public State handle(RequestVote message) throws IOException {
            if (message.getTerm() < meta().getCurrentTerm()) {
                logger.info("rejecting request vote msg by {} in {}, received stale {}.", message.getCandidate(),
                    meta().getCurrentTerm(), message.getTerm());
                send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta().getCurrentTerm()));
                return this;
            }
            if (message.getTerm() > meta().getCurrentTerm()) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(),
                    meta().getCurrentTerm());
                return stay(meta().withTerm(message.getTerm())).gotoFollower().handle(message);
            }
            logger.info("rejecting requestVote msg by {} in {}, already voted for {}", message.getCandidate(), meta()
                .getCurrentTerm(), meta().getVotedFor());
            send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta().getCurrentTerm()));
            return this;
        }

        @Override
        public State handle(VoteCandidate message) throws IOException {
            RaftMetadata meta = meta();
            if (message.getTerm() < meta.getCurrentTerm()) {
                logger.info("ignore vote candidate msg by {} in {}, received stale {}.", message.getMember(), meta
                    .getCurrentTerm(), message.getTerm());
                return this;
            }
            if (message.getTerm() > meta.getCurrentTerm()) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(),
                    meta.getCurrentTerm());
                return stay(meta.withTerm(message.getTerm())).gotoFollower();
            }

            meta = meta.incVote();
            if (meta.hasMajority()) {
                logger.info("received vote by {}, won election with {} of {} votes", message.getMember(), meta
                    .getVotesReceived(), meta.getConfig().members().size());
                return stay(meta).gotoLeader();
            } else {
                logger.info("received vote by {}, have {} of {} votes", message.getMember(), meta.getVotesReceived(),
                    meta.getConfig().members().size());
                return stay(meta);
            }
        }

        @Override
        public State handle(DeclineCandidate message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(),
                    meta().getCurrentTerm());
                return stay(meta().withTerm(message.getTerm())).gotoFollower();
            } else {
                logger.info("candidate is declined by {} in term {}", message.getMember(), meta().getCurrentTerm());
                return this;
            }
        }

        @Override
        public State handle(AppendEntries message) throws IOException {
            boolean leaderIsAhead = message.getTerm() >= meta().getCurrentTerm();
            if (leaderIsAhead) {
                logger.info("reverting to follower, because got append entries from leader in {}, but am in {}",
                    message.getTerm(), meta().getCurrentTerm());
                return stay(meta().withTerm(message.getTerm())).gotoFollower().handle(message);
            } else {
                return this;
            }
        }

        @Override
        public State handle(InstallSnapshot message) throws IOException {
            boolean leaderIsAhead = message.getTerm() >= meta().getCurrentTerm();
            if (leaderIsAhead) {
                logger.info("reverting to follower, because got install snapshot from leader in {}, but am in {}",
                    message.getTerm(), meta().getCurrentTerm());
                return stay(meta().withTerm(message.getTerm())).gotoFollower().handle(message);
            } else {
                send(message.getLeader(), new InstallSnapshotRejected(clusterDiscovery.self(), meta().getCurrentTerm()));
                return this;
            }
        }

        public State electionTimeout() throws IOException {
            logger.info("voting timeout, starting a new election (among {})", meta().getConfig().members().size());
            return stay(meta().forNewElection()).beginElection();
        }
    }

    private class LeaderState extends State {

        public LeaderState(RaftMetadata meta) throws IOException {
            super(meta);
        }

        @Override
        public RaftState state() {
            return Leader;
        }

        @Override
        protected LeaderState stay(RaftMetadata meta) throws IOException {
            super.stay(meta);
            return this;
        }

        private State gotoFollower() throws IOException {
            context.cancelTimer(RaftContext.SEND_HEARTBEAT);
            return new FollowerState(this.meta().forFollower()).resetElectionDeadline();
        }

        public State selfJoin() throws IOException {
            logger.info("bootstrap cluster with {}", clusterDiscovery.self());
            RaftMetadata meta = meta()
                .withTerm(meta().getCurrentTerm() + 1)
                .withConfig(new StableClusterConfiguration(clusterDiscovery.self()));

            nextIndex = new LogIndexMap(replicatedLog.lastIndex() + 1);
            matchIndex = new LogIndexMap(0);
            replicationIndex = ImmutableMap.of();
            final LogEntry entry;
            if (replicatedLog.isEmpty()) {
                entry = new LogEntry(meta.getConfig(), meta.getCurrentTerm(), replicatedLog.nextIndex(),
                    clusterDiscovery.self());
            } else {
                entry = new LogEntry(Noop.INSTANCE, meta.getCurrentTerm(), replicatedLog.nextIndex(),
                    clusterDiscovery.self());
            }

            replicatedLog.append(entry);
            matchIndex.put(clusterDiscovery.self(), entry.getIndex());

            sendHeartbeat();
            startHeartbeat();

            ClientMessage clientMessage;
            while ((clientMessage = stashed.poll()) != null) {
                logger.debug("appending command: [{}] from {} to replicated log", clientMessage.getCmd(),
                    clientMessage.getClient());
                LogEntry logEntry = new LogEntry(clientMessage.getCmd(), meta.getCurrentTerm(), replicatedLog
                    .nextIndex(), clientMessage.getClient());
                replicatedLog.append(logEntry);
                matchIndex.put(clusterDiscovery.self(), entry.getIndex());
            }

            return stay(meta).maybeCommitEntry();
        }

        public State elected() throws IOException {
            logger.info("became leader for {}", meta().getCurrentTerm());

            // for each server, index of the next log entry
            // to send to that server (initialized to leader
            // last log index + 1)
            nextIndex = new LogIndexMap(replicatedLog.lastIndex() + 1);

            // for each server, index of highest log entry
            // known to be replicated on server
            // (initialized to 0, increases monotonically)
            matchIndex = new LogIndexMap(0);

            // for each server store last send heartbeat time
            // 0 if no response is expected
            replicationIndex = ImmutableMap.of();

            final LogEntry entry;
            if (replicatedLog.isEmpty()) {
                entry = new LogEntry(meta().getConfig(), meta().getCurrentTerm(), replicatedLog.nextIndex(),
                    clusterDiscovery.self());
            } else {
                entry = new LogEntry(Noop.INSTANCE, meta().getCurrentTerm(), replicatedLog.nextIndex(),
                    clusterDiscovery.self());
            }

            replicatedLog.append(entry);
            matchIndex.put(clusterDiscovery.self(), entry.getIndex());

            sendHeartbeat();
            startHeartbeat();

            ClientMessage clientMessage;
            while ((clientMessage = stashed.poll()) != null) {
                logger.debug("appending command: [{}] from {} to replicated log", clientMessage.getCmd(),
                    clientMessage.getClient());
                LogEntry logEntry = new LogEntry(clientMessage.getCmd(), meta().getCurrentTerm(), replicatedLog
                    .nextIndex(), clientMessage.getClient());
                replicatedLog.append(logEntry);
                matchIndex.put(clusterDiscovery.self(), entry.getIndex());
            }

            return maybeCommitEntry();
        }

        @Override
        public State handle(ClientMessage message) throws IOException {
            logger.debug("appending command: [{}] from {} to replicated log", message.getCmd(), message.getClient());
            LogEntry entry = new LogEntry(message.getCmd(), meta().getCurrentTerm(), replicatedLog.nextIndex(),
                message.getClient());
            replicatedLog.append(entry);
            matchIndex.put(clusterDiscovery.self(), entry.getIndex());
            sendHeartbeat();
            return maybeCommitEntry();
        }

        @Override
        public State handle(AppendEntries message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                logger.info("leader ({}) got append entries from fresher leader ({}), will step down and the leader " +
                    "will keep being: {}", meta().getCurrentTerm(), message.getTerm(), message.getMember());
                return gotoFollower().handle(message);
            } else {
                logger.warn("leader ({}) got append entries from rogue leader ({} @ {}), it's not fresher than self, " +
                        "will send entries, to force it to step down.", meta().getCurrentTerm(), message.getMember(),
                    message.getTerm());
                sendEntries(message.getMember());
                return this;
            }
        }

        @Override
        public State handle(AppendRejected message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                return stay(meta().withTerm(message.getTerm())).gotoFollower();
            }
            if (message.getTerm() == meta().getCurrentTerm()) {
                long nextIndexFor = nextIndex.indexFor(message.getMember());
                if (nextIndexFor > message.getLastIndex()) {
                    nextIndex.put(message.getMember(), message.getLastIndex());
                } else if (nextIndexFor > 0) {
                    nextIndex.decrementFor(message.getMember());
                }
                logger.warn("follower {} rejected write, term {}, decrement index to {}", message.getMember(),
                    message.getTerm(), nextIndex.indexFor(message.getMember()));
                sendEntries(message.getMember());
                return this;
            } else {
                logger.warn("follower {} rejected write: {}, ignore", message.getMember(), message.getTerm());
                return this;
            }
        }

        @Override
        public State handle(AppendSuccessful message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                return stay(meta().withTerm(message.getTerm())).gotoFollower();
            }
            if (message.getTerm() == meta().getCurrentTerm()) {
                logger.info("received append successful {} in term: {}", message, meta().getCurrentTerm());
                assert (message.getLastIndex() <= replicatedLog.lastIndex());
                if (message.getLastIndex() > 0) {
                    nextIndex.put(message.getMember(), message.getLastIndex() + 1);
                }
                matchIndex.putIfGreater(message.getMember(), message.getLastIndex());
                replicationIndex = Immutable.replace(replicationIndex, message.getMember(), 0L);
                maybeSendEntries(message.getMember());
                return maybeCommitEntry();
            } else {
                logger.warn("unexpected append successful: {} in term:{}", message, meta().getCurrentTerm());
                return this;
            }
        }

        @Override
        public State handle(InstallSnapshot message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                logger.info("leader ({}) got install snapshot from fresher leader ({}), will step down and the leader" +
                        " will keep being: {}",
                    meta().getCurrentTerm(), message.getTerm(), message.getLeader());
                return stay(meta().withTerm(message.getTerm())).gotoFollower().handle(message);
            } else {
                logger.info("rejecting install snapshot {}, current term is {}",
                    message.getTerm(), meta().getCurrentTerm());
                logger.warn("leader ({}) got install snapshot from rogue leader ({} @ {}), " +
                        "it's not fresher than self, will send entries, to force it to step down.",
                    meta().getCurrentTerm(), message.getLeader(), message.getTerm());
                sendEntries(message.getLeader());
                return this;
            }
        }

        @Override
        public State handle(InstallSnapshotSuccessful message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                return stay(meta().withTerm(message.getTerm())).gotoFollower();
            }
            if (message.getTerm() == meta().getCurrentTerm()) {
                logger.info("received install snapshot successful[{}], last index[{}]", message.getLastIndex(),
                    replicatedLog.lastIndex());
                assert (message.getLastIndex() <= replicatedLog.lastIndex());
                if (message.getLastIndex() > 0) {
                    nextIndex.put(message.getMember(), message.getLastIndex() + 1);
                }
                matchIndex.putIfGreater(message.getMember(), message.getLastIndex());
                return maybeCommitEntry();
            } else {
                logger.warn("unexpected install snapshot successful: {} in term:{}", message, meta().getCurrentTerm());
                return this;
            }
        }

        @Override
        public State handle(InstallSnapshotRejected message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                // since there seems to be another leader!
                return stay(meta().withTerm(message.getTerm())).gotoFollower();
            } else if (message.getTerm() == meta().getCurrentTerm()) {
                logger.info("follower {} rejected write: {}, back out the first index in this term and retry",
                    message.getMember(), message.getTerm());
                if (nextIndex.indexFor(message.getMember()) > 1) {
                    nextIndex.decrementFor(message.getMember());
                }
                sendEntries(message.getMember());
                return this;
            } else {
                logger.warn("unexpected install snapshot successful: {} in term:{}", message, meta().getCurrentTerm());
                return this;
            }
        }

        @Override
        public State handle(AddServer request) throws IOException {
            RaftMetadata meta = meta();
            if (meta.getConfig().isTransitioning()) {
                logger.warn("try add server {} in transitioning state", request.getMember());
                send(request.getMember(), new AddServerResponse(
                    AddServerResponse.Status.TIMEOUT,
                    Optional.of(clusterDiscovery.self())
                ));
                return stay(meta);
            } else {
                if (meta.members().contains(request.getMember())) {
                    send(request.getMember(), new AddServerResponse(
                        AddServerResponse.Status.OK,
                        Optional.of(clusterDiscovery.self())
                    ));
                    return stay(meta);
                }
                StableClusterConfiguration config = new StableClusterConfiguration(
                    Immutable.compose(meta.members(), request.getMember())
                );
                meta = meta.withConfig(meta.getConfig().transitionTo(config));
                return stay(meta).handle(new ClientMessage(request.getMember(), config));
            }
        }

        @Override
        public State handle(RemoveServer request) throws IOException {
            RaftMetadata meta = meta();
            if (meta.getConfig().isTransitioning()) {
                logger.warn("try remove server {} in transitioning state", request.getMember());
                send(request.getMember(), new RemoveServerResponse(
                    RemoveServerResponse.Status.TIMEOUT,
                    Optional.of(clusterDiscovery.self())
                ));
            } else {
                StableClusterConfiguration config = new StableClusterConfiguration(
                    meta.membersWithout(request.getMember())
                );
                meta.getConfig().transitionTo(config);
                meta = meta.withConfig(meta.getConfig().transitionTo(config));
                return stay(meta).handle(new ClientMessage(request.getMember(), config));
            }
            return stay(meta);
        }

        @Override
        public State handle(RequestVote message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                logger.info("received newer {}, current term is {}", message.getTerm(), meta().getCurrentTerm());
                return stay(meta().withTerm(message.getTerm())).gotoFollower().handle(message);
            } else {
                logger.warn("rejecting vote for {}, and {}, currentTerm: {}, received stale term number {}",
                    message.getCandidate(), message.getTerm(),
                    meta().getCurrentTerm(), message.getTerm());
                send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta().getCurrentTerm()));
                return this;
            }
        }

        private void startHeartbeat() {
            logger.info("starting heartbeat");
            context.startTimer(RaftContext.SEND_HEARTBEAT, heartbeat, heartbeat, () -> {
                lock.lock();
                try {
                    if (state == this) {
                        state = sendHeartbeat();
                    } else {
                        throw new IllegalStateException();
                    }
                } catch (IOException | IllegalStateException e) {
                    logger.error("error send heartbeat", e);
                } finally {
                    lock.unlock();
                }
            });
        }

        private LeaderState sendHeartbeat() throws IOException {
            logger.debug("send heartbeat: {}", meta().members());
            long timeout = System.currentTimeMillis() - heartbeat;
            for (DiscoveryNode member : meta().membersWithout(clusterDiscovery.self())) {
                // check heartbeat response timeout for prevent re-send heartbeat
                if (replicationIndex.getOrDefault(member, 0L) < timeout) {
                    sendEntries(member);
                }
            }
            return this;
        }

        private void maybeSendEntries(DiscoveryNode follower) throws IOException {
            // check heartbeat response timeout for prevent re-send heartbeat
            long timeout = System.currentTimeMillis() - heartbeat;
            if (replicationIndex.getOrDefault(follower, 0L) < timeout) {
                // if member is already append prev entries,
                // their next index must be equal to last index in log
                if (nextIndex.indexFor(follower) <= replicatedLog.lastIndex()) {
                    sendEntries(follower);
                }
            }
        }

        private void sendEntries(DiscoveryNode follower) throws IOException {
            RaftMetadata meta = meta();
            replicationIndex = Immutable.replace(replicationIndex, follower, System.currentTimeMillis());
            long lastIndex = nextIndex.indexFor(follower);

            if (replicatedLog.hasSnapshot()) {
                RaftSnapshot snapshot = replicatedLog.snapshot();
                if (snapshot.getMeta().getLastIncludedIndex() >= lastIndex) {
                    logger.info("send install snapshot to {} in term {}", follower, meta.getCurrentTerm());
                    send(follower, new InstallSnapshot(clusterDiscovery.self(), meta.getCurrentTerm(), snapshot));
                    return;
                }
            }

            if (lastIndex > replicatedLog.nextIndex()) {
                throw new Error("Unexpected from index " + lastIndex + " > " + replicatedLog.nextIndex());
            } else {
                ImmutableList<LogEntry> entries = replicatedLog.entriesBatchFrom(lastIndex, maxEntries);
                long prevIndex = Math.max(0, lastIndex - 1);
                long prevTerm = replicatedLog.termAt(prevIndex);
                logger.info("send to {} append entries {} prev {}:{} in {} from index:{}", follower, entries.size(),
                    prevTerm, prevIndex, meta.getCurrentTerm(), lastIndex);
                AppendEntries append = new AppendEntries(clusterDiscovery.self(), meta.getCurrentTerm(),
                    prevTerm, prevIndex,
                    entries,
                    replicatedLog.committedIndex());
                send(follower, append);
            }
        }

        private State maybeCommitEntry() throws IOException {
            RaftMetadata meta = meta();
            long indexOnMajority;
            while ((indexOnMajority = matchIndex.consensusForIndex(meta.getConfig())) > replicatedLog.committedIndex
                ()) {
                logger.debug("index of majority: {}", indexOnMajority);
                replicatedLog.slice(replicatedLog.committedIndex(), indexOnMajority);
                ImmutableList<LogEntry> entries = replicatedLog.slice(replicatedLog.committedIndex() + 1,
                    indexOnMajority);
                for (LogEntry entry : entries) {
                    logger.debug("committing log at index: {}", entry.getIndex());
                    replicatedLog.commit(entry.getIndex());
                    if (entry.getCommand() instanceof StableClusterConfiguration) {
                        StableClusterConfiguration config = (StableClusterConfiguration) entry.getCommand();
                        logger.info("apply new configuration, old: {}, new: {}", meta.getConfig(), config);
                        meta = meta.withConfig(config);
                        if (!meta.getConfig().containsOnNewState(clusterDiscovery.self())) {
                            return stay(meta).gotoFollower();
                        }
                    } else if (entry.getCommand() instanceof Noop) {
                        logger.trace("ignore noop entry");
                    } else {
                        logger.debug("applying command[index={}]: {}, will send result to client: {}", entry.getIndex
                            (), entry.getCommand().getClass(), entry.getClient());
                        Streamable result = registry.apply(entry.getIndex(), entry.getCommand());
                        if (result != null) {
                            send(entry.getClient(), result);
                        }
                    }
                }
            }
            if (replicatedLog.committedEntries() >= snapshotInterval) {
                return stay(meta).createSnapshot();
            } else {
                return stay(meta);
            }
        }
    }
}
