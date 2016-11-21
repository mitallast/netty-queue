package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.cluster.*;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.log.LogIndexMap;
import org.mitallast.queue.raft.log.ReplicatedLog;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;
import org.slf4j.MDC;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;

import static org.mitallast.queue.raft.RaftState.*;

public class Raft extends AbstractLifecycleComponent {

    private final TransportService transportService;
    private final TransportController transportController;
    private final ClusterDiscovery clusterDiscovery;
    private final ResourceFSM resourceFSM;

    private volatile Optional<DiscoveryNode> recentlyContactedByLeader;

    private volatile LogIndexMap nextIndex;
    private volatile LogIndexMap matchIndex;

    private final boolean bootstrap;
    private final long electionDeadline;
    private final long jointTimeout;
    private final long heartbeat;
    private final long snapshotInterval;

    private final RaftContext context;
    private final ConcurrentMap<String, ScheduledFuture> timerMap = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Streamable> stashed = new ConcurrentLinkedQueue<>();

    private final ImmutableMap<RaftState, Behavior> stateMap;
    private volatile State state;

    @Inject
    public Raft(
        Config config,
        TransportService transportService,
        TransportController transportController,
        ClusterDiscovery clusterDiscovery,
        ReplicatedLog replicatedLog,
        ResourceFSM resourceFSM,
        RaftContext context
    ) {
        super(config.getConfig("raft"), Raft.class);
        this.transportService = transportService;
        this.transportController = transportController;
        this.clusterDiscovery = clusterDiscovery;
        this.resourceFSM = resourceFSM;
        this.context = context;

        recentlyContactedByLeader = Optional.empty();
        nextIndex = new LogIndexMap(0);
        matchIndex = new LogIndexMap(0);

        bootstrap = this.config.getBoolean("bootstrap");
        electionDeadline = this.config.getDuration("election-deadline", TimeUnit.MILLISECONDS);
        jointTimeout = this.config.getDuration("joint-timeout", TimeUnit.MILLISECONDS);
        heartbeat = this.config.getDuration("heartbeat", TimeUnit.MILLISECONDS);
        snapshotInterval = this.config.getLong("snapshot-interval");

        stateMap = ImmutableMap.<RaftState, Behavior>builder()
            .put(Follower, new FollowerBehavior())
            .put(Candidate, new CandidateBehavior())
            .put(Leader, new LeaderBehavior())
            .build();

        state = new State(Follower, new RaftMetadata(replicatedLog));
    }

    @Override
    protected void doStart() {
        if (state.currentMetadata.replicatedLog().entries().isEmpty()) {
            if (bootstrap) {
                logger.info("bootstrap cluster");
                receive(new ChangeConfiguration(new StableClusterConfiguration(0, clusterDiscovery.self())));
            } else {
                logger.info("joint cluster");
                receive(JointTimeout.INSTANCE);
            }
        } else {
            logger.info("restarted node");
            receive(ApplyCommittedLog.INSTANCE);
        }
    }

    @Override
    protected void doStop() {
        stopHeartbeat();
    }

    @Override
    protected void doClose() {
    }

    // fsm related

    private void onTransition(RaftState prevState, RaftState newState) {
        if (prevState == Leader) {
            stopHeartbeat();
        }
        if (newState == Follower) {
            resetElectionDeadline();
            stateMap.get(state.currentState).unstashAll();
        } else if (newState == Candidate) {
            receive(BeginElection.INSTANCE);
            resetElectionDeadline();
        } else if (newState == Leader) {
            receive(ElectedAsLeader.INSTANCE);
            cancelElectionDeadline();
        }
    }

    private void cancelTimer(String timerName) {
        ScheduledFuture timer = timerMap.remove(timerName);
        if (timer != null) {
            timer.cancel(false);
        }
    }

    private void setTimer(String timerName, Streamable event, long timeout, TimeUnit timeUnit) {
        setTimer(timerName, event, timeout, timeUnit, false);
    }

    private void setTimer(String timerName, Streamable event, long timeout, TimeUnit timeUnit, boolean repeat) {
        cancelTimer(timerName);
        final ScheduledFuture timer;
        if (repeat) {
            timer = context.scheduleAtFixedRate(() -> receive(event), timeout, timeout, timeUnit);
        } else {
            timer = context.schedule(() -> receive(event), timeout, timeUnit);
        }
        timerMap.put(timerName, timer);
    }

    public void receive(Streamable event) {
        context.execute(() -> {
            try (MDC.MDCCloseable ignore = MDC.putCloseable("state", this.state.currentState.name())) {
                MDC.put("state", this.state.currentState.name());
                State prevState = this.state;
                State newState = stateMap.get(this.state.currentState).receiveMessage(event, this.state.currentMetadata);
                if (newState == null) {
                    logger.warn("unhandled event: {} in state {}", event, this.state.currentState);
                } else {
                    this.state = newState;
                    if (!this.state.currentState.equals(prevState.currentState)) {
                        logger.info("transition {} to {}", prevState.currentState, this.state.currentState);
                        onTransition(prevState.currentState, this.state.currentState);
                    }
                }
            } catch (Exception e) {
                logger.error("unexpected error in fsm", e);
            }
        });
    }

    public RaftState currentState() {
        return state.currentState;
    }

    public RaftMetadata currentMeta() {
        return state.currentMetadata;
    }

    public ReplicatedLog currentLog() {
        return state.currentMetadata.replicatedLog();
    }

    public ImmutableList<Streamable> currentStashed() {
        return ImmutableList.copyOf(stashed);
    }

    private State stay() {
        return state.stay();
    }

    private State stay(RaftMetadata newMeta) {
        return state.stay(newMeta);
    }

    private State goTo(RaftState newState, RaftMetadata newMeta) {
        return state.goTo(newState, newMeta);
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
        logger.info("reset election deadline");
        cancelElectionDeadline();
        long timeout = new Random().nextInt((int) (electionDeadline / 2)) + electionDeadline;
        setTimer("raft-election-timeout", ElectionTimeout.INSTANCE, timeout, TimeUnit.MILLISECONDS);
    }

    private void send(DiscoveryNode node, Streamable message) {
        if (node.equals(clusterDiscovery.self())) {
            transportController.dispatch(new MessageTransportFrame(Version.CURRENT, message));
            receive(message);
        } else {
            transportService.connectToNode(node);
            transportService.channel(node).message(message);
        }
    }

    private boolean leaderIsLagging(AppendEntries msg, RaftMetadata meta) {
        return msg.getTerm().less(meta.getCurrentTerm());
    }

    private void senderIsCurrentLeader(DiscoveryNode leader) {
        logger.info("leader is {}", leader);
        recentlyContactedByLeader = Optional.of(leader);
    }

    // additional classes

    @FunctionalInterface
    private interface BehaviorHandler<T extends Streamable> {
        State handle(T message, RaftMetadata meta);
    }

    private abstract class Behavior {

        private final ImmutableMap<Class, BehaviorHandler> handlerMap;

        public Behavior() {
            ImmutableMap.Builder<Class, BehaviorHandler> builder = ImmutableMap.builder();
            when(builder, ChangeConfiguration.class, this::handle);
            when(builder, AppendEntries.class, this::handle);
            when(builder, InitLogSnapshot.class, this::handle);
            when(builder, ClientMessage.class, this::handle);
            when(builder, RequestVote.class, this::handle);
            when(builder, InstallSnapshot.class, this::handle);
            when(builder, ElectionTimeout.class, this::handle);
            when(builder, BeginElection.class, this::handle);
            when(builder, VoteCandidate.class, this::handle);
            when(builder, DeclineCandidate.class, this::handle);
            when(builder, ElectedAsLeader.class, this::handle);
            when(builder, SendHeartbeat.class, this::handle);
            when(builder, AppendRejected.class, this::handle);
            when(builder, AppendSuccessful.class, this::handle);
            when(builder, InstallSnapshotSuccessful.class, this::handle);
            when(builder, InstallSnapshotRejected.class, this::handle);
            when(builder, ApplyCommittedLog.class, this::handle);
            when(builder, JointRequest.class, this::handle);
            when(builder, JointTimeout.class, this::handle);
            handlerMap = builder.build();
        }

        private <T extends Streamable> void when(
            ImmutableMap.Builder<Class, BehaviorHandler> builder,
            Class<T> messageClass,
            BehaviorHandler<T> handler) {
            builder.put(messageClass, handler);
        }

        public State handle(ChangeConfiguration message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(AppendEntries message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(InitLogSnapshot message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public abstract State handle(ClientMessage message, RaftMetadata meta);

        public State handle(RequestVote message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(InstallSnapshot message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(ElectionTimeout message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(BeginElection message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(VoteCandidate message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(DeclineCandidate message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(ElectedAsLeader message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(SendHeartbeat message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(AppendRejected message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(AppendSuccessful message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(InstallSnapshotSuccessful message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(InstallSnapshotRejected message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(ApplyCommittedLog message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(JointTimeout message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(JointRequest message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        @SuppressWarnings("unchecked")
        public State receiveMessage(Streamable event, RaftMetadata currentMetadata) {
            BehaviorHandler handler = handlerMap.get(event.getClass());
            if (handler != null) {
                return handler.handle(event, currentMetadata);
            } else {
                return null;
            }
        }

        public void stash(Streamable streamable) {
            logger.info("stash {}", streamable);
            stashed.add(streamable);
        }

        public abstract void unstashAll();
    }

    private abstract class ClusterManagementBehavior extends Behavior {

        @Override
        public State handle(JointTimeout message, RaftMetadata meta) {
            if (meta.getConfig().members().isEmpty()) {
                logger.info("joint timeout");
                for (DiscoveryNode node : clusterDiscovery.discoveryNodes()) {
                    if (!node.equals(clusterDiscovery.self())) {
                        try {
                            send(node, new JointRequest(clusterDiscovery.self()));
                        } catch (Exception e) {
                            logger.warn("error connect to {}", node);
                        }
                    }
                }
                setTimer("joint-timeout", JointTimeout.INSTANCE, jointTimeout, TimeUnit.MILLISECONDS);
            } else {
                cancelTimer("joint-timeout");
            }
            return stay();
        }

        @Override
        public State handle(ChangeConfiguration message, RaftMetadata meta) {
            if (bootstrap && meta.getConfig().members().isEmpty() && message.getNewConf().containsOnNewState(clusterDiscovery.self())) {
                logger.info("bootstrap cluster with {}", message.getNewConf());
                return goTo(Leader, meta.withConfig(message.getNewConf()).forLeader());
            } else {
                ClusterConfiguration transitioningConfig = meta.getConfig().transitionTo(message.getNewConf());
                if (!transitioningConfig.transitionToStable().members().equals(meta.getConfig().members())) {
                    logger.info("starting transition to new configuration, old [size: {}]: {}, migrating to [size: {}]: {}",
                        meta.getConfig().members().size(), meta.getConfig().members(),
                        transitioningConfig.transitionToStable().members().size(),
                        transitioningConfig
                    );
                    return handle(new ClientMessage(clusterDiscovery.self(), transitioningConfig), meta);
                } else {
                    return stay(meta);
                }
            }
        }
    }

    private abstract class SnapshotBehavior extends ClusterManagementBehavior {
        @Override
        public State handle(InitLogSnapshot message, RaftMetadata meta) {
            long committedIndex = meta.replicatedLog().committedIndex();
            RaftSnapshotMetadata snapshotMeta = new RaftSnapshotMetadata(meta.replicatedLog().termAt(committedIndex), committedIndex, meta.getConfig());
            logger.info("init snapshot up to: {}:{}", snapshotMeta.getLastIncludedIndex(), snapshotMeta.getLastIncludedTerm());

            Optional<RaftSnapshot> snapshot = resourceFSM.prepareSnapshot(snapshotMeta);
            if (snapshot.isPresent()) {
                logger.info("successfully prepared snapshot for {}:{}, compacting log now", snapshotMeta.getLastIncludedIndex(), snapshotMeta.getLastIncludedTerm());
                meta.withLog(meta.replicatedLog().compactedWith(snapshot.get(), clusterDiscovery.self()));
            }

            return stay(meta);
        }
    }

    private class FollowerBehavior extends SnapshotBehavior {
        @Override
        public State handle(ClientMessage message, RaftMetadata meta) {
            if (recentlyContactedByLeader.isPresent()) {
                send(recentlyContactedByLeader.get(), message);
            } else {
                stash(message);
            }
            return stay();
        }

        @Override
        public State handle(RequestVote message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("received newer {}, current term is {}", message.getTerm(), meta.getCurrentTerm());
                meta = meta.withTerm(message.getTerm());
            }
            if (meta.canVoteIn(message.getTerm())) {
                resetElectionDeadline();
                if (meta.replicatedLog().lastTerm().filter(term -> message.getLastLogTerm().less(term)).isPresent()) {
                    logger.warn("rejecting vote for {} at term {}, candidate's lastLogTerm: {} < ours: {}",
                        message.getCandidate(),
                        message.getTerm(),
                        message.getLastLogTerm(),
                        meta.replicatedLog().lastTerm());
                    send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta.getCurrentTerm()));
                    return stay(meta);
                }
                if (meta.replicatedLog().lastTerm().filter(term -> term.equals(message.getLastLogTerm())).isPresent() &&
                    message.getLastLogIndex() < meta.replicatedLog().lastIndex()) {
                    logger.warn("rejecting vote for {} at term {}, candidate's lastLogIndex: {} < ours: {}",
                        message.getCandidate(),
                        message.getTerm(),
                        message.getLastLogIndex(),
                        meta.replicatedLog().lastIndex());
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
        public State handle(AppendEntries message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("received newer {}, current term is {}", message.getTerm(), meta.getCurrentTerm());
                meta = meta.withTerm(message.getTerm());
            }
            // 1) Reply false if term < currentTerm (5.1)
            if (meta.getCurrentTerm().less(message.getTerm())) {
                logger.warn("rejecting write (old term): {} < {} ", message.getTerm(), meta.getCurrentTerm());
                send(message.getMember(), new AppendRejected(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay(meta);
            }
            // 2) Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (5.3)
            if (!meta.replicatedLog().containsMatchingEntry(message.getPrevLogTerm(), message.getPrevLogIndex())) {
                logger.warn("rejecting write (inconsistent log): {}:{} {} ", message.getPrevLogTerm(), message.getPrevLogIndex(), meta.replicatedLog());
                send(message.getMember(), new AppendRejected(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay(meta);
            } else {
                return appendEntries(message, meta);
            }
        }

        @SuppressWarnings("StatementWithEmptyBody")
        @Override
        public State handle(ApplyCommittedLog message, RaftMetadata meta) {
            ImmutableList<LogEntry> committed = meta.replicatedLog().slice(0, meta.replicatedLog().committedIndex());
            for (LogEntry entry : committed) {
                logger.info("committing entry {} on follower, leader is committed until [{}]", entry, meta.replicatedLog().committedIndex());
                if (entry.getTerm().greater(meta.getCurrentTerm())) {
                    meta = meta.withTerm(entry.getTerm());
                }
                if (entry.getCommand() instanceof ClusterConfiguration) {
                    ClusterConfiguration config = (ClusterConfiguration) entry.getCommand();
                    meta = meta.withConfig(config);
                } else if (entry.getCommand() instanceof Noop) {
                    // ignore
                } else if (entry.getCommand() instanceof RaftSnapshot) {
                    RaftSnapshot snapshot = (RaftSnapshot) entry.getCommand();
                    meta = meta.withConfig(snapshot.getMeta().getConfig());
                    meta = meta.withLog(meta.replicatedLog().compactedWith(snapshot, clusterDiscovery.self()));
                    resourceFSM.apply(snapshot.getData());
                    logger.warn("unexpected raft snapshot in log");
                } else {
                    resourceFSM.apply(entry.getCommand());
                }
            }
            return stay(meta);
        }

        @Override
        public State handle(InstallSnapshot message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("received newer {}, current term is {}", message.getTerm(), meta.getCurrentTerm());
                meta = meta.withTerm(message.getTerm());
            }
            if (message.getTerm().less(meta.getCurrentTerm())) {
                logger.info("rejecting install snapshot {}, current term is {}", message.getTerm(), meta.getCurrentTerm());
                send(message.getLeader(), new InstallSnapshotRejected(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay(meta);
            } else {
                resetElectionDeadline();
                logger.info("got snapshot from {}, is for: {}", message.getLeader(), message.getSnapshot().getMeta());

                meta = meta.withConfig(message.getSnapshot().getMeta().getConfig())
                    .withLog(meta.replicatedLog().compactedWith(message.getSnapshot(), clusterDiscovery.self()));
                resourceFSM.apply(message.getSnapshot().getData());

                logger.info("response snapshot installed in {} last index {}", meta.getCurrentTerm(), meta.replicatedLog().lastIndex());
                send(message.getLeader(), new InstallSnapshotSuccessful(clusterDiscovery.self(), meta.getCurrentTerm(), meta.replicatedLog().lastIndex()));

                return stay(meta);
            }
        }

        @Override
        public State handle(ElectionTimeout message, RaftMetadata meta) {
            resetElectionDeadline();
            if (meta.getConfig().members().isEmpty()) {
                logger.info("no members found, keep state follower");
                return goTo(Follower, meta.forFollower());
            } else {
                return goTo(Candidate, meta.forNewElection());
            }
        }

        @SuppressWarnings("StatementWithEmptyBody")
        private State appendEntries(AppendEntries msg, RaftMetadata meta) {
            if (leaderIsLagging(msg, meta)) {
                logger.info("rejecting write (Leader is lagging) of: {} {}", msg, meta.replicatedLog());
                send(msg.getMember(), new AppendRejected(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay(meta);
            }

            senderIsCurrentLeader(msg.getMember());
            unstashAll();

            if (!msg.getEntries().isEmpty()) {
                // If an existing entry conflicts with a new one (same index
                // but different terms), delete the existing entry and all that
                // follow it (5.3)

                // Append any new entries not already in the log

                long prevIndex = msg.getEntries().get(0).getIndex() - 1;
                logger.info("append({}, {}) to {}", msg.getEntries(), prevIndex, meta.replicatedLog());
                meta = meta.withLog(meta.replicatedLog().append(msg.getEntries(), prevIndex));
            }
            logger.info("response append successful term:{} lastIndex:{}", meta.getCurrentTerm(), meta.replicatedLog().lastIndex());
            AppendSuccessful response = new AppendSuccessful(clusterDiscovery.self(), meta.getCurrentTerm(), meta.replicatedLog().lastIndex());
            send(msg.getMember(), response);

            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

            if (msg.getLeaderCommit() > meta.replicatedLog().committedIndex()) {
                ImmutableList<LogEntry> entries = meta.replicatedLog().slice(meta.replicatedLog().committedIndex() + 1, msg.getLeaderCommit());
                for (LogEntry entry : entries) {
                    if (entry.getCommand() instanceof ClusterConfiguration) {
                        meta = meta.withConfig((ClusterConfiguration) entry.getCommand());
                    } else if (entry.getCommand() instanceof Noop) {
                        // ignore
                    } else if (entry.getCommand() instanceof RaftSnapshot) {
                        logger.warn("unexpected raft snapshot in log");
                    } else {
                        logger.info("committing entry {} on follower, leader is committed until [{}]", entry, msg.getLeaderCommit());
                        resourceFSM.apply(entry.getCommand());
                    }
                    meta = meta.withLog(meta.replicatedLog().commit(entry.getIndex()));
                }
            }

            if (meta.replicatedLog().committedEntries() >= snapshotInterval) {
                receive(InitLogSnapshot.INSTANCE);
            }

            ClusterConfiguration config = msg.getEntries().stream()
                .map(LogEntry::getCommand)
                .filter(cmd -> cmd instanceof ClusterConfiguration)
                .map(cmd -> (ClusterConfiguration) cmd)
                .reduce(meta.getConfig(), (a, b) -> b);

            resetElectionDeadline();

            return stay(meta.withTerm(meta.replicatedLog().lastTerm()).withConfig(config));
        }

        @Override
        public void unstashAll() {
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

    private class CandidateBehavior extends SnapshotBehavior {
        @Override
        public State handle(ClientMessage message, RaftMetadata meta) {
            stash(message);
            return stay();
        }

        @Override
        public State handle(BeginElection message, RaftMetadata meta) {
            if (meta.getConfig().members().size() < 1) {
                logger.warn("unable to initialize election, don't know enough nodes");
                return goTo(Follower, meta.forFollower());
            } else if (meta.getConfig().members().equals(ImmutableSet.of(clusterDiscovery.self()))) {
                logger.info("self-elect as leader");
                return goTo(Leader, meta.forLeader());
            } else {
                logger.info("initializing election (among {} nodes) for {}", meta.getConfig().members().size(), meta.getCurrentTerm());
                RequestVote request = new RequestVote(meta.getCurrentTerm(), clusterDiscovery.self(), meta.replicatedLog().lastTerm().orElseGet(() -> new Term(0)), meta.replicatedLog().lastIndex());
                for (DiscoveryNode member : meta.membersWithout(clusterDiscovery.self())) {
                    logger.info("send request vote to {}", member);
                    send(member, request);
                }
                return stay(meta.incVote().withVoteFor(clusterDiscovery.self()));
            }
        }

        @Override
        public State handle(RequestVote message, RaftMetadata meta) {
            if (message.getTerm().less(meta.getCurrentTerm())) {
                logger.info("rejecting request vote msg by {} in {}, received stale {}.", message.getCandidate(), meta.getCurrentTerm(), message.getTerm());
                send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta.getCurrentTerm()));
                return stay();
            }
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(), meta.getCurrentTerm());
                receive(message);
                return goTo(Follower, meta.withTerm(message.getTerm()).forFollower());
            }
            logger.info("rejecting requestVote msg by {} in {}, already voted for {}", message.getCandidate(), meta.getCurrentTerm(), meta.getVotedFor());
            send(message.getCandidate(), new DeclineCandidate(clusterDiscovery.self(), meta.getCurrentTerm()));
            return stay();
        }

        @Override
        public State handle(VoteCandidate message, RaftMetadata meta) {
            if (message.getTerm().less(meta.getCurrentTerm())) {
                logger.info("ignore vote candidate msg by {} in {}, received stale {}.", message.getMember(), meta.getCurrentTerm(), message.getTerm());
                return stay();
            }
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(), meta.getCurrentTerm());
                return goTo(Follower, meta.withTerm(message.getTerm()).forFollower());
            }

            meta = meta.incVote();
            if (meta.hasMajority()) {
                logger.info("received vote by {}, won election with {} of {} votes", message.getMember(), meta.getVotesReceived(), meta.getConfig().members().size());
                return goTo(Leader, meta.forLeader());
            } else {
                logger.info("received vote by {}, have {} of {} votes", message.getMember(), meta.getVotesReceived(), meta.getConfig().members().size());
                return stay(meta);
            }
        }

        @Override
        public State handle(DeclineCandidate message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(), meta.getCurrentTerm());
                return goTo(Follower, meta.withTerm(message.getTerm()).forFollower());
            } else {
                logger.info("candidate is declined by {} in term {}", message.getMember(), meta.getCurrentTerm());
                return stay();
            }
        }

        @Override
        public State handle(AppendEntries message, RaftMetadata meta) {
            boolean leaderIsAhead = message.getTerm().greaterOrEqual(meta.getCurrentTerm());
            if (leaderIsAhead) {
                logger.info("reverting to follower, because got append entries from leader in {}, but am in {}", message.getTerm(), meta.getCurrentTerm());
                receive(message);
                return goTo(Follower, meta.forFollower());
            } else {
                return stay();
            }
        }

        @Override
        public State handle(ElectionTimeout message, RaftMetadata meta) {
            if (meta.getConfig().members().size() > 1) {
                logger.info("voting timeout, starting a new election (among {})", meta.getConfig().members().size());
                receive(BeginElection.INSTANCE);
                return stay(meta.forNewElection());
            } else {
                logger.info("voting timeout, unable to start election, don't know enough nodes (members: {})...", meta.getConfig().members().size());
                return goTo(Follower, meta.forFollower());
            }
        }

        @Override
        public void unstashAll() {
            logger.warn("try unstash in state candidate");
        }
    }

    private class LeaderBehavior extends SnapshotBehavior {

        @Override
        public State handle(ElectedAsLeader message, RaftMetadata meta) {
            logger.info("became leader for {}", meta.getCurrentTerm());

            // for each server, index of the next log entry
            // to send to that server (initialized to leader
            // last log index + 1)
            nextIndex = new LogIndexMap(meta.replicatedLog().lastIndex() + 1);

            // for each server, index of highest log entry
            // known to be replicated on server
            // (initialized to 0, increases monotonically)
            matchIndex = new LogIndexMap(0);

            final State newState;
            if (meta.replicatedLog().entries().isEmpty()) {
                newState = handle(new ClientMessage(clusterDiscovery.self(), meta.getConfig()), meta);
            } else {
                newState = handle(new ClientMessage(clusterDiscovery.self(), Noop.INSTANCE), meta);
            }

            if (newState.currentState != Leader) {
                return newState;
            }

            sendHeartbeat(newState.currentMetadata);
            startHeartbeat();
            unstashAll();

            return newState;
        }

        @Override
        public State handle(SendHeartbeat message, RaftMetadata meta) {
            sendHeartbeat(meta);
            return stay();
        }

        @Override
        public State handle(ClientMessage message, RaftMetadata meta) {
            logger.info("appending command: [{}] from {} to replicated log", message.getCmd(), message.getClient());

            LogEntry entry = new LogEntry(message.getCmd(), meta.getCurrentTerm(), meta.replicatedLog().nextIndex(), message.getClient());

            logger.info("adding {} to log {}", entry, meta.replicatedLog());
            meta = meta.withLog(meta.replicatedLog().append(entry));
            matchIndex.put(clusterDiscovery.self(), entry.getIndex());
            logger.debug("log status = {}", meta.replicatedLog());
            return maybeCommitEntry(meta, matchIndex);
        }

        @Override
        public State handle(AppendEntries message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("leader ({}) got append entries from fresher leader ({}), will step down and the leader will keep being: {}", meta.getCurrentTerm(), message.getTerm(), message.getMember());
                receive(message);
                return goTo(Follower, meta.forFollower());
            } else {
                logger.warn("leader ({}) got append entries from rogue leader ({} @ {}), it's not fresher than self, will send entries, to force it to step down.", meta.getCurrentTerm(), message.getMember(), message.getTerm());
                sendEntries(message.getMember(), meta);
                return stay();
            }
        }

        @Override
        public State handle(AppendRejected message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                return goTo(Follower, meta.withTerm(message.getTerm()).forFollower());
            }
            if (message.getTerm().equals(meta.getCurrentTerm())) {
                if (nextIndex.indexFor(message.getMember()) > 1) {
                    nextIndex.decrementFor(message.getMember());
                }
                logger.warn("follower {} rejected write, term {}, decrement index to {}", message.getMember(), message.getTerm(), nextIndex.indexFor(message.getMember()));
                sendEntries(message.getMember(), meta);
                return stay();
            } else {
                logger.warn("follower {} rejected write: {}, ignore", message.getMember(), message.getTerm());
                return stay();
            }
        }

        @Override
        public State handle(AppendSuccessful message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                return goTo(Follower, meta.withTerm(message.getTerm()).forFollower());
            }
            if (message.getTerm().equals(meta.getCurrentTerm())) {
                logger.info("received append successful {} in term: {}", message, meta.getCurrentTerm());
                assert (message.getLastIndex() <= meta.replicatedLog().lastIndex());
                if (message.getLastIndex() > 0) {
                    nextIndex.put(message.getMember(), message.getLastIndex() + 1);
                }
                matchIndex.putIfGreater(message.getMember(), message.getLastIndex());
                return maybeCommitEntry(meta, matchIndex);
            } else {
                logger.warn("unexpected append successful: {} in term:{}", message, meta.getCurrentTerm());
                return stay();
            }
        }

        @Override
        public State handle(InstallSnapshot message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("leader ({}) got install snapshot from fresher leader ({}), will step down and the leader will keep being: {}",
                    meta.getCurrentTerm(), message.getTerm(), message.getLeader());
                receive(message);
                return goTo(Follower, meta.withTerm(message.getTerm()).forFollower());
            } else {
                logger.info("rejecting install snapshot {}, current term is {}",
                    message.getTerm(), meta.getCurrentTerm());
                logger.warn("leader ({}) got install snapshot from rogue leader ({} @ {}), " +
                        "it's not fresher than self, will send entries, to force it to step down.",
                    meta.getCurrentTerm(), message.getLeader(), message.getTerm());
                sendEntries(message.getLeader(), meta);
                return stay();
            }
        }

        @Override
        public State handle(InstallSnapshotSuccessful message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                return goTo(Follower, meta.withTerm(message.getTerm()).forFollower());
            }
            if (message.getTerm().equals(meta.getCurrentTerm())) {
                logger.info("received install snapshot successful[{}], last index[{}]", message.getLastIndex(), meta.replicatedLog().lastIndex());
                assert (message.getLastIndex() <= meta.replicatedLog().lastIndex());
                if (message.getLastIndex() > 0) {
                    nextIndex.put(message.getMember(), message.getLastIndex() + 1);
                }
                matchIndex.putIfGreater(message.getMember(), message.getLastIndex());
                return maybeCommitEntry(meta, matchIndex);
            } else {
                logger.warn("unexpected install snapshot successful: {} in term:{}", message, meta.getCurrentTerm());
                return stay();
            }
        }

        @Override
        public State handle(InstallSnapshotRejected message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                // since there seems to be another leader!
                return goTo(Follower, meta.withTerm(message.getTerm()).forFollower());
            } else if (message.getTerm().equals(meta.getCurrentTerm())) {
                logger.info("follower {} rejected write: {}, back out the first index in this term and retry", message.getMember(), message.getTerm());
                if (nextIndex.indexFor(message.getMember()) > 1) {
                    nextIndex.decrementFor(message.getMember());
                }
                sendEntries(message.getMember(), meta);
                return stay();
            } else {
                logger.warn("unexpected install snapshot successful: {} in term:{}", message, meta.getCurrentTerm());
                return stay();
            }
        }

        @Override
        public State handle(JointRequest message, RaftMetadata meta) {
            if (meta.getConfig().isTransitioning()) {
                logger.warn("ignore joint request {} in transitioning state", message.member());
                return stay();
            } else if (meta.getConfig().members().contains(message.member())) {
                logger.warn("ignore joint request {}, already contains", message.member());
                return stay();
            } else {
                StableClusterConfiguration config = (StableClusterConfiguration) meta.getConfig();
                return handle(new ChangeConfiguration(config.withMember(message.member())), meta);
            }
        }

        private void sendHeartbeat(RaftMetadata meta) {
            logger.info("send heartbeat: {}", meta.members());
            for (DiscoveryNode member : meta.membersWithout(clusterDiscovery.self())) {
                sendEntries(member, meta);
            }
        }

        private void sendEntries(DiscoveryNode follower, RaftMetadata meta) {
            long lastIndex = nextIndex.indexFor(follower);

            if (meta.replicatedLog().hasSnapshot()) {
                RaftSnapshot snapshot = meta.replicatedLog().snapshot();
                if (snapshot.getMeta().getLastIncludedIndex() >= lastIndex) {
                    logger.info("send install snapshot to {} in term {}", follower, meta.getCurrentTerm());
                    send(follower, new InstallSnapshot(clusterDiscovery.self(), meta.getCurrentTerm(), snapshot));
                    return;
                }
            }

            if (lastIndex > meta.replicatedLog().nextIndex()) {
                throw new Error("Unexpected from index " + lastIndex + " > " + meta.replicatedLog().nextIndex());
            } else {
                ImmutableList<LogEntry> entries = meta.replicatedLog().entriesBatchFrom(lastIndex);
                long prevIndex = Math.max(0, lastIndex - 1);
                Term prevTerm = meta.replicatedLog().termAt(prevIndex);
                logger.info("send append entries {} prev {}:{} in {} from index:{}", entries, prevTerm, prevIndex, meta.getCurrentTerm(), lastIndex);
                AppendEntries append = new AppendEntries(clusterDiscovery.self(), meta.getCurrentTerm(),
                    prevTerm, prevIndex,
                    entries,
                    meta.replicatedLog().committedIndex());
                send(follower, append);
            }
        }

        private State maybeCommitEntry(RaftMetadata meta, LogIndexMap matchIndex) {
            long indexOnMajority;
            while ((indexOnMajority = matchIndex.consensusForIndex(meta.getConfig())) > meta.replicatedLog().committedIndex()) {
                logger.info("index of majority: {}", indexOnMajority);
                meta.replicatedLog().slice(meta.replicatedLog().committedIndex(), indexOnMajority);
                ImmutableList<LogEntry> entries = meta.replicatedLog().slice(meta.replicatedLog().committedIndex() + 1, indexOnMajority);
                for (LogEntry entry : entries) {
                    logger.info("committing log at index: {}", entry.getIndex());
                    meta = meta.withLog(meta.replicatedLog().commit(entry.getIndex()));
                    if (entry.getCommand() instanceof JointConsensusClusterConfiguration) {
                        JointConsensusClusterConfiguration config = (JointConsensusClusterConfiguration) entry.getCommand();
                        logger.info("transition to new configuration, old: {}, migrating to: {}", meta.getConfig(), config);
                        meta = meta.withConfig(config);
                        if (!meta.getConfig().containsOnNewState(clusterDiscovery.self())) {
                            return goTo(Follower, meta.forFollower());
                        } else {
                            receive(new ClientMessage(clusterDiscovery.self(), meta.getConfig().transitionToStable()));
                        }
                        break;
                    } else if (entry.getCommand() instanceof StableClusterConfiguration) {
                        StableClusterConfiguration config = (StableClusterConfiguration) entry.getCommand();
                        logger.info("apply new configuration, old: {}, new: {}", meta.getConfig(), config);
                        meta = meta.withConfig(config);
                        if (!meta.getConfig().containsOnNewState(clusterDiscovery.self())) {
                            return goTo(Follower, meta.forFollower());
                        }
                    } else if (entry.getCommand() instanceof Noop) {
                        logger.trace("ignore noop entry");
                    } else {
                        logger.trace("applying command[index={}]: {}, will send result to client: {}", entry.getIndex(), entry.getCommand(), entry.getClient());
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
        public void unstashAll() {
            Streamable poll;
            while ((poll = stashed.poll()) != null) {
                receive(poll);
            }
        }
    }

    private class State {
        private final RaftState currentState;
        private final RaftMetadata currentMetadata;

        private State(RaftState currentState, RaftMetadata currentMetadata) {
            this.currentState = currentState;
            this.currentMetadata = currentMetadata;
        }

        public State stay() {
            return this;
        }

        public State stay(RaftMetadata newMetadata) {
            return new State(currentState, newMetadata);
        }

        public State goTo(RaftState newState, RaftMetadata newMetadata) {
            return new State(newState, newMetadata);
        }

        @Override
        public String toString() {
            return "State{" +
                "currentState=" + currentState +
                ", currentMetadata=" + currentMetadata +
                '}';
        }
    }
}
