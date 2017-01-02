package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.persistent.PersistentService;
import org.mitallast.queue.raft.persistent.ReplicatedLog;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;
import org.slf4j.MDC;

import java.io.IOError;
import java.io.IOException;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;

import static org.mitallast.queue.raft.RaftState.*;

public class Raft extends AbstractLifecycleComponent {

    private static final ImmutableMap<Class<? extends Streamable>, StateConsumer> consumerMap =
        ImmutableMap.<Class<? extends Streamable>, StateConsumer>builder()
            .put(AppendEntries.class, (state, event) -> state.handle((AppendEntries) event))
            .put(AppendRejected.class, (state, event) -> state.handle((AppendRejected) event))
            .put(AppendSuccessful.class, (state, event) -> state.handle((AppendSuccessful) event))
            .put(ElectionTimeout.class, (state, event) -> state.handle((ElectionTimeout) event))
            .put(BeginElection.class, (state, event) -> state.handle((BeginElection) event))
            .put(RequestVote.class, (state, event) -> state.handle((RequestVote) event))
            .put(VoteCandidate.class, (state, event) -> state.handle((VoteCandidate) event))
            .put(DeclineCandidate.class, (state, event) -> state.handle((DeclineCandidate) event))
            .put(ElectedAsLeader.class, (state, event) -> state.handle((ElectedAsLeader) event))
            .put(SendHeartbeat.class, (state, event) -> state.handle((SendHeartbeat) event))
            .put(ClientMessage.class, (state, event) -> state.handle((ClientMessage) event))
            .put(InitLogSnapshot.class, (state, event) -> state.handle((InitLogSnapshot) event))
            .put(InstallSnapshot.class, (state, event) -> state.handle((InstallSnapshot) event))
            .put(InstallSnapshotSuccessful.class, (state, event) -> state.handle((InstallSnapshotSuccessful) event))
            .put(InstallSnapshotRejected.class, (state, event) -> state.handle((InstallSnapshotRejected) event))
            .put(AddServer.class, (state, event) -> state.handle((AddServer) event))
            .put(AddServerResponse.class, (state, event) -> state.handle((AddServerResponse) event))
            .put(RemoveServer.class, (state, event) -> state.handle((RemoveServer) event))
            .put(RemoveServerResponse.class, (state, event) -> state.handle((RemoveServerResponse) event))
            .build();

    private final TransportService transportService;
    private final TransportController transportController;
    private final ClusterDiscovery clusterDiscovery;
    private final PersistentService persistentService;
    private final ReplicatedLog replicatedLog;
    private final ResourceFSM resourceFSM;
    private final boolean bootstrap;
    private final long electionDeadline;
    private final long heartbeat;
    private final long snapshotInterval;
    private final RaftContext context;
    private final ConcurrentMap<String, ScheduledFuture> timerMap = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Streamable> stashed = new ConcurrentLinkedQueue<>();
    private volatile Optional<DiscoveryNode> recentlyContactedByLeader;
    private volatile ImmutableMap<DiscoveryNode, Long> replicationIndex;
    private volatile LogIndexMap nextIndex;
    private volatile LogIndexMap matchIndex;
    private volatile State state;

    @Inject
    public Raft(
        Config config,
        TransportService transportService,
        TransportController transportController,
        ClusterDiscovery clusterDiscovery,
        PersistentService persistentService,
        ResourceFSM resourceFSM,
        RaftContext context
    ) throws IOException {
        super(config.getConfig("raft"), Raft.class);
        this.transportService = transportService;
        this.transportController = transportController;
        this.clusterDiscovery = clusterDiscovery;
        this.persistentService = persistentService;
        this.replicatedLog = persistentService.openLog();
        this.resourceFSM = resourceFSM;
        this.context = context;

        recentlyContactedByLeader = Optional.empty();
        nextIndex = new LogIndexMap(0);
        matchIndex = new LogIndexMap(0);

        bootstrap = this.config.getBoolean("bootstrap");
        electionDeadline = this.config.getDuration("election-deadline", TimeUnit.MILLISECONDS);
        heartbeat = this.config.getDuration("heartbeat", TimeUnit.MILLISECONDS);
        snapshotInterval = this.config.getLong("snapshot-interval");
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
        stopHeartbeat();
    }

    @Override
    protected void doClose() {
    }

    // fsm related

    private State onTransition(State prevState, State newState) throws IOException {
        if (prevState.state() == Leader) {
            stopHeartbeat();
        }
        if (newState.state() == Follower) {
            resetElectionDeadline();
            newState = newState.unstash();
        } else if (newState.state() == Candidate) {
            resetElectionDeadline();
            newState = newState.handle(BeginElection.INSTANCE);
        } else if (newState.state() == Leader) {
            cancelElectionDeadline();
            newState = newState.handle(ElectedAsLeader.INSTANCE);
        }
        return newState;
    }

    private void cancelTimer(String timerName) {
        ScheduledFuture timer = timerMap.remove(timerName);
        if (timer != null) {
            timer.cancel(false);
        }
    }

    private void setTimer(String timerName, Streamable event, long timeout, TimeUnit timeUnit, boolean repeat) {
        cancelTimer(timerName);
        final ScheduledFuture timer;
        if (repeat) {
            timer = context.scheduleAtFixedRate(() -> apply(event), timeout, timeout, timeUnit);
        } else {
            timer = context.schedule(() -> apply(event), timeout, timeUnit);
        }
        timerMap.put(timerName, timer);
    }

    public synchronized void apply(Streamable event) {
        try {
            state = state.apply(event);
        } catch (IOException e) {
            logger.error("error apply event", e);
            throw new IOError(e);
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

    private void stopHeartbeat() {
        cancelTimer("raft-heartbeat");
    }

    private void startHeartbeat() {
        logger.info("starting heartbeat");
        setTimer("raft-heartbeat", SendHeartbeat.INSTANCE, heartbeat, TimeUnit.MILLISECONDS, true);
    }

    private void cancelElectionDeadline() {
        cancelTimer("raft-election-timeout");
    }

    private void resetElectionDeadline() {
        logger.debug("reset election deadline");
        cancelElectionDeadline();
        long timeout = new Random().nextInt((int) (electionDeadline / 2)) + electionDeadline;
        setTimer("raft-election-timeout", ElectionTimeout.INSTANCE, timeout, TimeUnit.MILLISECONDS, false);
    }

    private void send(DiscoveryNode node, Streamable message) {
        if (node.equals(clusterDiscovery.self())) {
            transportController.dispatch(new MessageTransportFrame(Version.CURRENT, message));
        } else {
            try {
                transportService.connectToNode(node);
                transportService.channel(node).message(message);
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

    @FunctionalInterface
    private interface StateConsumer {
        State apply(State state, Streamable event) throws IOException;
    }

    private abstract class State {
        private final RaftMetadata meta;

        private State(RaftMetadata meta) throws IOException {
            this.meta = meta;
            persistentService.updateState(meta.getCurrentTerm(), meta.getVotedFor());
        }

        public abstract RaftState state();

        public RaftMetadata meta() {
            return meta;
        }

        public State stay() {
            return this;
        }

        public abstract State stay(RaftMetadata meta) throws IOException;

        // replication

        public State apply(Streamable message) throws IOException {
            try (MDC.MDCCloseable ignore = MDC.putCloseable("state", state().name())) {
                return consumerMap.get(message.getClass()).apply(this, message);
            }
        }

        public abstract State handle(AppendEntries message) throws IOException;

        public State handle(AppendRejected message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        public State handle(AppendSuccessful message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        // election

        public State handle(ElectionTimeout message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        public State handle(BeginElection message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        public abstract State handle(RequestVote message) throws IOException;

        public State handle(VoteCandidate message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        public State handle(DeclineCandidate message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        public State handle(ElectedAsLeader message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        // leader

        public State handle(SendHeartbeat message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        public abstract State handle(ClientMessage message) throws IOException;

        // snapshot

        @SuppressWarnings("unused")
        public State handle(InitLogSnapshot message) throws IOException {
            long committedIndex = replicatedLog.committedIndex();
            RaftSnapshotMetadata snapshotMeta = new RaftSnapshotMetadata(replicatedLog.termAt(committedIndex), committedIndex, meta.getConfig());
            logger.info("init snapshot up to: {}:{}", snapshotMeta.getLastIncludedIndex(), snapshotMeta.getLastIncludedTerm());

            Optional<RaftSnapshot> snapshot = resourceFSM.prepareSnapshot(snapshotMeta);
            if (snapshot.isPresent()) {
                logger.info("successfully prepared snapshot for {}:{}, compacting log now", snapshotMeta.getLastIncludedIndex(), snapshotMeta.getLastIncludedTerm());
                replicatedLog.compactWith(snapshot.get(), clusterDiscovery.self());
            }

            return stay(meta);
        }

        public abstract State handle(InstallSnapshot message) throws IOException;

        public State handle(InstallSnapshotSuccessful message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        public State handle(InstallSnapshotRejected message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        // joint consensus

        public abstract State handle(AddServer request) throws IOException;

        public State handle(AddServerResponse message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        public abstract State handle(RemoveServer request) throws IOException;

        public State handle(RemoveServerResponse message) throws IOException {
            logger.debug("unhandled: {} in {}", message, state());
            return stay();
        }

        // stash messages

        public void stash(Streamable streamable) throws IOException {
            logger.debug("stash {}", streamable);
            stashed.add(streamable);
        }

        public abstract State unstash() throws IOException;
    }

    private class FollowerState extends State {

        public FollowerState(RaftMetadata meta) throws IOException {
            super(meta);
        }

        @Override
        public RaftState state() {
            return Follower;
        }

        @Override
        public State stay(RaftMetadata meta) throws IOException {
            return new FollowerState(meta);
        }

        public State gotoCandidate(RaftMetadata meta) throws IOException {
            return onTransition(this, new CandidateState(meta));
        }

        public State gotoLeader(RaftMetadata meta) throws IOException {
            return onTransition(this, new LeaderState(meta));
        }

        public State initialize() throws IOException {
            if (replicatedLog.isEmpty()) {
                if (bootstrap) {
                    logger.info("bootstrap cluster");
                    return apply(new AddServer(clusterDiscovery.self()));
                } else {
                    logger.info("joint cluster");
                    return apply(ElectionTimeout.INSTANCE);
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
        public State handle(ClientMessage message) throws IOException {
            if (recentlyContactedByLeader.isPresent()) {
                send(recentlyContactedByLeader.get(), message);
            } else {
                stash(message);
            }
            return stay();
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
                send(message.getMember(), new AppendRejected(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay(meta);
            }
            // 2) Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (5.3)
            if (!replicatedLog.containsMatchingEntry(message.getPrevLogTerm(), message.getPrevLogIndex())) {
                logger.warn("rejecting write (inconsistent log): {}:{} {} ", message.getPrevLogTerm(), message.getPrevLogIndex(), replicatedLog);
                send(message.getMember(), new AppendRejected(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay(meta);
            } else {
                return appendEntries(message, meta);
            }
        }

        private State appendEntries(AppendEntries msg, RaftMetadata meta) throws IOException {
            senderIsCurrentLeader(msg.getMember());

            if (!msg.getEntries().isEmpty()) {
                // If an existing entry conflicts with a new one (same index
                // but different terms), delete the existing entry and all that
                // follow it (5.3)

                // Append any new entries not already in the log

                long prevIndex = msg.getEntries().get(0).getIndex() - 1;
                logger.debug("append({}, {})", msg.getEntries(), prevIndex);
                replicatedLog.append(msg.getEntries(), prevIndex);
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
                        resourceFSM.apply(entry.getCommand());
                    }
                    replicatedLog.commit(entry.getIndex());
                }
            }


            ClusterConfiguration config = msg.getEntries().stream()
                .map(LogEntry::getCommand)
                .filter(cmd -> cmd instanceof ClusterConfiguration)
                .map(cmd -> (ClusterConfiguration) cmd)
                .reduce(meta.getConfig(), (a, b) -> b);

            resetElectionDeadline();

            State newState = stay(meta.withTerm(replicatedLog.lastTerm()).withConfig(config)).unstash();
            if (replicatedLog.committedEntries() >= snapshotInterval) {
                return newState.apply(InitLogSnapshot.INSTANCE);
            } else {
                return newState;
            }
        }

        @Override
        public State handle(ElectionTimeout message) throws IOException {
            resetElectionDeadline();
            if (meta().getConfig().members().isEmpty()) {
                logger.info("no members found, joint timeout");
                for (DiscoveryNode node : clusterDiscovery.discoveryNodes()) {
                    if (!node.equals(clusterDiscovery.self())) {
                        send(node, new AddServer(clusterDiscovery.self()));
                    }
                }
                return stay();
            } else {
                return gotoCandidate(meta().forNewElection());
            }
        }

        // joint consensus

        @Override
        public State handle(AddServer request) throws IOException {
            if (bootstrap && meta().getConfig().members().isEmpty() &&
                request.getMember().equals(clusterDiscovery.self()) &&
                replicatedLog.isEmpty()) {
                logger.info("bootstrap cluster with {}", request.getMember());
                return gotoLeader(meta().withTerm(meta().getCurrentTerm() + 1).withConfig(new StableClusterConfiguration(request.getMember())));
            }
            send(request.getMember(), new AddServerResponse(
                AddServerResponse.Status.NOT_LEADER,
                recentlyContactedByLeader
            ));
            return stay();
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
            return stay();
        }

        @Override
        public State handle(RemoveServer request) throws IOException {
            send(request.getMember(), new RemoveServerResponse(
                RemoveServerResponse.Status.NOT_LEADER,
                recentlyContactedByLeader
            ));
            return stay();
        }

        public State handle(RemoveServerResponse request) throws IOException {
            if (request.getStatus() == RemoveServerResponse.Status.OK) {
                logger.info("successful removed");
            }
            recentlyContactedByLeader = request.getLeader();
            resetElectionDeadline();
            return stay();
        }

        @Override
        public State handle(InstallSnapshot message) throws IOException {
            RaftMetadata meta = meta();
            if (message.getTerm() > meta.getCurrentTerm()) {
                logger.info("received newer {}, current term is {}", message.getTerm(), meta.getCurrentTerm());
                meta = meta.withTerm(message.getTerm());
            }
            if (message.getTerm() < meta.getCurrentTerm()) {
                logger.info("rejecting install snapshot {}, current term is {}", message.getTerm(), meta.getCurrentTerm());
                send(message.getLeader(), new InstallSnapshotRejected(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay(meta);
            } else {
                resetElectionDeadline();
                logger.info("got snapshot from {}, is for: {}", message.getLeader(), message.getSnapshot().getMeta());

                meta = meta.withConfig(message.getSnapshot().getMeta().getConfig());
                replicatedLog.compactWith(message.getSnapshot(), clusterDiscovery.self());
                resourceFSM.apply(message.getSnapshot().getData());

                logger.info("response snapshot installed in {} last index {}", meta.getCurrentTerm(), replicatedLog.lastIndex());
                send(message.getLeader(), new InstallSnapshotSuccessful(clusterDiscovery.self(), meta.getCurrentTerm(), replicatedLog.lastIndex()));

                return stay(meta);
            }
        }

        @Override
        public State unstash() throws IOException {
            if (recentlyContactedByLeader.isPresent()) {
                DiscoveryNode leader = recentlyContactedByLeader.get();
                Streamable poll;
                while ((poll = stashed.poll()) != null) {
                    send(leader, poll);
                }
            } else {
                logger.warn("try unstash without leader");
            }
            return stay();
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
        public State stay(RaftMetadata meta) throws IOException {
            return new CandidateState(meta);
        }

        public State gotoFollower(RaftMetadata meta) throws IOException {
            return onTransition(this, new FollowerState(meta));
        }

        public State gotoLeader(RaftMetadata meta) throws IOException {
            return onTransition(this, new LeaderState(meta));
        }

        @Override
        public State handle(ClientMessage message) throws IOException {
            stash(message);
            return stay();
        }

        @Override
        public State handle(AddServer request) throws IOException {
            send(request.getMember(), new AddServerResponse(
                AddServerResponse.Status.NOT_LEADER,
                Optional.empty()
            ));
            return stay();
        }

        @Override
        public State handle(RemoveServer request) throws IOException {
            send(request.getMember(), new RemoveServerResponse(
                RemoveServerResponse.Status.NOT_LEADER,
                Optional.empty()
            ));
            return stay();
        }

        @Override
        public State handle(BeginElection message) throws IOException {
            RaftMetadata meta = meta();
            logger.info("initializing election (among {} nodes) for {}", meta.getConfig().members().size(), meta.getCurrentTerm());
            RequestVote request = new RequestVote(meta.getCurrentTerm(), clusterDiscovery.self(), replicatedLog.lastTerm().orElse(0L), replicatedLog.lastIndex());
            for (DiscoveryNode member : meta.membersWithout(clusterDiscovery.self())) {
                logger.info("send request vote to {}", member);
                send(member, request);
            }
            meta = meta.incVote().withVoteFor(clusterDiscovery.self());
            if (meta.hasMajority()) {
                logger.info("received vote by {}, won election with {} of {} votes", clusterDiscovery.self(), meta.getVotesReceived(), meta.getConfig().members().size());
                return gotoLeader(meta.forLeader());
            } else {
                return stay(meta);
            }
        }

        @Override
        public State handle(RequestVote message) throws IOException {
            if (message.getTerm() < meta().getCurrentTerm()) {
                logger.info("rejecting request vote msg by {} in {}, received stale {}.", message.getCandidate(), meta().getCurrentTerm(), message.getTerm());
                send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta().getCurrentTerm()));
                return stay();
            }
            if (message.getTerm() > meta().getCurrentTerm()) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(), meta().getCurrentTerm());
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower()).apply(message);
            }
            logger.info("rejecting requestVote msg by {} in {}, already voted for {}", message.getCandidate(), meta().getCurrentTerm(), meta().getVotedFor());
            send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta().getCurrentTerm()));
            return stay();
        }

        @Override
        public State handle(VoteCandidate message) throws IOException {
            RaftMetadata meta = meta();
            if (message.getTerm() < meta.getCurrentTerm()) {
                logger.info("ignore vote candidate msg by {} in {}, received stale {}.", message.getMember(), meta.getCurrentTerm(), message.getTerm());
                return stay();
            }
            if (message.getTerm() > meta.getCurrentTerm()) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(), meta.getCurrentTerm());
                return gotoFollower(meta.withTerm(message.getTerm()).forFollower());
            }

            meta = meta.incVote();
            if (meta.hasMajority()) {
                logger.info("received vote by {}, won election with {} of {} votes", message.getMember(), meta.getVotesReceived(), meta.getConfig().members().size());
                return gotoLeader(meta.forLeader());
            } else {
                logger.info("received vote by {}, have {} of {} votes", message.getMember(), meta.getVotesReceived(), meta.getConfig().members().size());
                return stay(meta);
            }
        }

        @Override
        public State handle(DeclineCandidate message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(), meta().getCurrentTerm());
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower());
            } else {
                logger.info("candidate is declined by {} in term {}", message.getMember(), meta().getCurrentTerm());
                return stay();
            }
        }

        @Override
        public State handle(AppendEntries message) throws IOException {
            boolean leaderIsAhead = message.getTerm() >= meta().getCurrentTerm();
            if (leaderIsAhead) {
                logger.info("reverting to follower, because got append entries from leader in {}, but am in {}", message.getTerm(), meta().getCurrentTerm());
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower()).apply(message);
            } else {
                return stay();
            }
        }

        @Override
        public State handle(InstallSnapshot message) throws IOException {
            boolean leaderIsAhead = message.getTerm() >= meta().getCurrentTerm();
            if (leaderIsAhead) {
                logger.info("reverting to follower, because got install snapshot from leader in {}, but am in {}", message.getTerm(), meta().getCurrentTerm());
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower()).apply(message);
            } else {
                send(message.getLeader(), new InstallSnapshotRejected(clusterDiscovery.self(), meta().getCurrentTerm()));
                return stay();
            }
        }

        @Override
        public State handle(ElectionTimeout message) throws IOException {
            logger.info("voting timeout, starting a new election (among {})", meta().getConfig().members().size());
            return stay(meta().forNewElection()).apply(BeginElection.INSTANCE);
        }

        @Override
        public State unstash() {
            logger.warn("try unstash in state candidate");
            return stay();
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
        public State stay(RaftMetadata meta) throws IOException {
            return new LeaderState(meta);
        }

        public State gotoFollower(RaftMetadata meta) throws IOException {
            return onTransition(this, new FollowerState(meta));
        }

        @Override
        public State handle(ElectedAsLeader message) throws IOException {
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
                entry = new LogEntry(meta().getConfig(), meta().getCurrentTerm(), replicatedLog.nextIndex(), clusterDiscovery.self());
            } else {
                entry = new LogEntry(Noop.INSTANCE, meta().getCurrentTerm(), replicatedLog.nextIndex(), clusterDiscovery.self());
            }

            replicatedLog.append(entry);
            matchIndex.put(clusterDiscovery.self(), entry.getIndex());

            sendHeartbeat(meta());
            startHeartbeat();

            return maybeCommitEntry(meta()).unstash();
        }

        @Override
        public State handle(SendHeartbeat message) throws IOException {
            sendHeartbeat(meta());
            return stay();
        }

        @Override
        public State handle(ClientMessage message) throws IOException {
            logger.debug("appending command: [{}] from {} to replicated log", message.getCmd(), message.getClient());
            LogEntry entry = new LogEntry(message.getCmd(), meta().getCurrentTerm(), replicatedLog.nextIndex(), message.getClient());
            replicatedLog.append(entry);
            matchIndex.put(clusterDiscovery.self(), entry.getIndex());
            sendHeartbeat(meta());
            return maybeCommitEntry(meta());
        }

        @Override
        public State handle(AppendEntries message) throws IOException {
            RaftMetadata meta = meta();
            if (message.getTerm() > meta.getCurrentTerm()) {
                logger.info("leader ({}) got append entries from fresher leader ({}), will step down and the leader will keep being: {}", meta.getCurrentTerm(), message.getTerm(), message.getMember());
                return gotoFollower(meta.forFollower()).apply(message);
            } else {
                logger.warn("leader ({}) got append entries from rogue leader ({} @ {}), it's not fresher than self, will send entries, to force it to step down.", meta.getCurrentTerm(), message.getMember(), message.getTerm());
                sendEntries(message.getMember(), meta);
                return stay();
            }
        }

        @Override
        public State handle(AppendRejected message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower());
            }
            if (message.getTerm() == meta().getCurrentTerm()) {
                if (nextIndex.indexFor(message.getMember()) > 0) {
                    nextIndex.decrementFor(message.getMember());
                }
                logger.warn("follower {} rejected write, term {}, decrement index to {}", message.getMember(), message.getTerm(), nextIndex.indexFor(message.getMember()));
                sendEntries(message.getMember(), meta());
                return stay();
            } else {
                logger.warn("follower {} rejected write: {}, ignore", message.getMember(), message.getTerm());
                return stay();
            }
        }

        @Override
        public State handle(AppendSuccessful message) throws IOException {
            RaftMetadata meta = meta();
            if (message.getTerm() > meta.getCurrentTerm()) {
                return gotoFollower(meta.withTerm(message.getTerm()).forFollower());
            }
            if (message.getTerm() == meta.getCurrentTerm()) {
                logger.debug("received append successful {} in term: {}", message, meta.getCurrentTerm());
                assert (message.getLastIndex() <= replicatedLog.lastIndex());
                if (message.getLastIndex() > 0) {
                    nextIndex.put(message.getMember(), message.getLastIndex() + 1);
                }
                matchIndex.putIfGreater(message.getMember(), message.getLastIndex());
                replicationIndex = Immutable.replace(replicationIndex, message.getMember(), 0L);
                maybeSendEntries(message.getMember(), meta);
                return maybeCommitEntry(meta);
            } else {
                logger.warn("unexpected append successful: {} in term:{}", message, meta.getCurrentTerm());
                return stay();
            }
        }

        @Override
        public State handle(InstallSnapshot message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                logger.info("leader ({}) got install snapshot from fresher leader ({}), will step down and the leader will keep being: {}",
                    meta().getCurrentTerm(), message.getTerm(), message.getLeader());
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower()).apply(message);
            } else {
                logger.info("rejecting install snapshot {}, current term is {}",
                    message.getTerm(), meta().getCurrentTerm());
                logger.warn("leader ({}) got install snapshot from rogue leader ({} @ {}), " +
                        "it's not fresher than self, will send entries, to force it to step down.",
                    meta().getCurrentTerm(), message.getLeader(), message.getTerm());
                sendEntries(message.getLeader(), meta());
                return stay();
            }
        }

        @Override
        public State handle(InstallSnapshotSuccessful message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower());
            }
            if (message.getTerm() == meta().getCurrentTerm()) {
                logger.info("received install snapshot successful[{}], last index[{}]", message.getLastIndex(), replicatedLog.lastIndex());
                assert (message.getLastIndex() <= replicatedLog.lastIndex());
                if (message.getLastIndex() > 0) {
                    nextIndex.put(message.getMember(), message.getLastIndex() + 1);
                }
                matchIndex.putIfGreater(message.getMember(), message.getLastIndex());
                return maybeCommitEntry(meta());
            } else {
                logger.warn("unexpected install snapshot successful: {} in term:{}", message, meta().getCurrentTerm());
                return stay();
            }
        }

        @Override
        public State handle(InstallSnapshotRejected message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                // since there seems to be another leader!
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower());
            } else if (message.getTerm() == meta().getCurrentTerm()) {
                logger.info("follower {} rejected write: {}, back out the first index in this term and retry", message.getMember(), message.getTerm());
                if (nextIndex.indexFor(message.getMember()) > 1) {
                    nextIndex.decrementFor(message.getMember());
                }
                sendEntries(message.getMember(), meta());
                return stay();
            } else {
                logger.warn("unexpected install snapshot successful: {} in term:{}", message, meta().getCurrentTerm());
                return stay();
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
                StableClusterConfiguration config = new StableClusterConfiguration(
                    Immutable.compose(meta.members(), request.getMember())
                );
                meta = meta.withConfig(meta.getConfig().transitionTo(config));
                return stay(meta).apply(new ClientMessage(request.getMember(), config));
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
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower()).apply(message);
            } else {
                logger.warn("rejecting vote for {}, and {}, currentTerm: {}, received stale term number {}",
                    message.getCandidate(), message.getTerm(),
                    meta().getCurrentTerm(), message.getTerm());
                send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta().getCurrentTerm()));
                return stay();
            }
        }

        private void sendHeartbeat(RaftMetadata meta) throws IOException {
            logger.debug("send heartbeat: {}", meta.members());
            long timeout = System.currentTimeMillis() - heartbeat;
            for (DiscoveryNode member : meta.membersWithout(clusterDiscovery.self())) {
                // check heartbeat response timeout for prevent re-send heartbeat
                if (replicationIndex.getOrDefault(member, 0L) < timeout) {
                    sendEntries(member, meta);
                }
            }
        }

        private void maybeSendEntries(DiscoveryNode follower, RaftMetadata meta) throws IOException {
            // check heartbeat response timeout for prevent re-send heartbeat
            long timeout = System.currentTimeMillis() - heartbeat;
            if (replicationIndex.getOrDefault(follower, 0L) < timeout) {
                // if member is already append prev entries,
                // their next index must be equal to last index in log
                if (nextIndex.indexFor(follower) == replicatedLog.lastIndex()) {
                    sendEntries(follower, meta);
                }
            }
        }

        private void sendEntries(DiscoveryNode follower, RaftMetadata meta) throws IOException {
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
                ImmutableList<LogEntry> entries = replicatedLog.entriesBatchFrom(lastIndex, 1000);
                long prevIndex = Math.max(0, lastIndex - 1);
                long prevTerm = replicatedLog.termAt(prevIndex);
                logger.info("send to {} append entries {} prev {}:{} in {} from index:{}", follower, entries.size(), prevTerm, prevIndex, meta.getCurrentTerm(), lastIndex);
                AppendEntries append = new AppendEntries(clusterDiscovery.self(), meta.getCurrentTerm(),
                    prevTerm, prevIndex,
                    entries,
                    replicatedLog.committedIndex());
                send(follower, append);
            }
        }

        private State maybeCommitEntry(RaftMetadata meta) throws IOException {
            long indexOnMajority;
            while ((indexOnMajority = matchIndex.consensusForIndex(meta.getConfig())) > replicatedLog.committedIndex()) {
                logger.debug("index of majority: {}", indexOnMajority);
                replicatedLog.slice(replicatedLog.committedIndex(), indexOnMajority);
                ImmutableList<LogEntry> entries = replicatedLog.slice(replicatedLog.committedIndex() + 1, indexOnMajority);
                for (LogEntry entry : entries) {
                    logger.debug("committing log at index: {}", entry.getIndex());
                    replicatedLog.commit(entry.getIndex());
                    if (entry.getCommand() instanceof StableClusterConfiguration) {
                        StableClusterConfiguration config = (StableClusterConfiguration) entry.getCommand();
                        logger.info("apply new configuration, old: {}, new: {}", meta.getConfig(), config);
                        meta = meta.withConfig(config);
                        if (!meta.getConfig().containsOnNewState(clusterDiscovery.self())) {
                            return gotoFollower(meta.forFollower());
                        }
                    } else if (entry.getCommand() instanceof Noop) {
                        logger.trace("ignore noop entry");
                    } else {
                        logger.debug("applying command[index={}]: {}, will send result to client: {}", entry.getIndex(), entry.getCommand().getClass(), entry.getClient());
                        Streamable result = resourceFSM.apply(entry.getCommand());
                        if (result != null) {
                            send(entry.getClient(), result);
                        }
                    }
                }
            }
            return stay(meta);
        }

        @Override
        public State unstash() throws IOException {
            Streamable poll = stashed.poll();
            if (poll != null) {
                return apply(poll);
            } else {
                return stay();
            }
        }
    }
}
