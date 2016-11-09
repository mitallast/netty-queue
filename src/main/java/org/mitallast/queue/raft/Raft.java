package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.cluster.*;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.log.LogIndexMap;
import org.mitallast.queue.raft.log.ReplicatedLog;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.TransportServer;
import org.mitallast.queue.transport.TransportService;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;

import static org.mitallast.queue.raft.RaftState.*;

public class Raft extends AbstractLifecycleComponent {

    private final TransportService transportService;
    private final TransportServer transportServer;
    private final ClusterDiscovery clusterDiscovery;
    private final ResourceFSM resourceFSM;

    private volatile Optional<DiscoveryNode> recentlyContactedByLeader;

    private volatile ReplicatedLog replicatedLog;
    private volatile LogIndexMap nextIndex = new LogIndexMap();
    private volatile LogIndexMap matchIndex = new LogIndexMap();

    private final int keepInitUntilFound;
    private final long electionDeadline;
    private final long discoveryTimeout;
    private final long heartbeat;
    private final long snapshotInterval;

    private volatile long nextIndexDefault = 1;

    private final DefaultEventExecutor context;
    private final ConcurrentMap<String, ScheduledFuture> timerMap = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Streamable> stashed = new ConcurrentLinkedQueue<>();

    private final ImmutableMap<RaftState, Behavior> stateMap;
    private volatile State state;

    @Inject
    public Raft(
        Config config,
        TransportService transportService,
        TransportServer transportServer,
        ClusterDiscovery clusterDiscovery,
        ReplicatedLog replicatedLog,
        ResourceFSM resourceFSM
    ) {
        super(config.getConfig("raft"), Raft.class);
        this.transportService = transportService;
        this.transportServer = transportServer;
        this.clusterDiscovery = clusterDiscovery;
        this.resourceFSM = resourceFSM;
        this.replicatedLog = replicatedLog;

        recentlyContactedByLeader = Optional.empty();
        nextIndex = new LogIndexMap();
        matchIndex = new LogIndexMap();

        keepInitUntilFound = this.config.getInt("keep-init-until-found");
        electionDeadline = this.config.getDuration("election-deadline", TimeUnit.MILLISECONDS);
        discoveryTimeout = this.config.getDuration("discovery-timeout", TimeUnit.MILLISECONDS);
        heartbeat = this.config.getDuration("heartbeat", TimeUnit.MILLISECONDS);
        snapshotInterval = this.config.getLong("snapshot-interval");

        context = new DefaultEventExecutor(new DefaultThreadFactory("raft", true, Thread.MAX_PRIORITY));

        stateMap = ImmutableMap.<RaftState, Behavior>builder()
            .put(Init, new InitialBehavior())
            .put(Follower, new FollowerBehavior())
            .put(Candidate, new CandidateBehavior())
            .put(Leader, new LeaderBehavior())
            .build();

        state = new State(Init, new RaftMetadata());
    }

    @Override
    protected void doStart() {
        if (replicatedLog.entries().isEmpty()) {
            receive(RaftMembersDiscoveryTimeout.INSTANCE);
            receive(new RaftMemberAdded(self(), keepInitUntilFound));
        } else {
            receive(ApplyCommittedLog.INSTANCE);
        }
    }

    @Override
    protected void doStop() {
        stopHeartbeat();
    }

    @Override
    protected void doClose() throws IOException {
        context.shutdownGracefully();
    }

    // fsm related

    private void onTransition(RaftState prevState, RaftState newState) {
        if (prevState == Init && newState == Follower) {
            cancelTimer("raft-discovery-timeout");
            resetElectionDeadline();
        } else if (prevState == Follower && newState == Candidate) {
            receive(BeginElection.INSTANCE);
            resetElectionDeadline();
        } else if (prevState == Candidate && newState == Leader) {
            receive(ElectedAsLeader.INSTANCE);
            cancelElectionDeadline();
        } else if (prevState == Leader) {
            stopHeartbeat();
        } else if (newState == Follower) {
            resetElectionDeadline();
            stateMap.get(state.currentState).unstashAll();
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

    @SuppressWarnings("UnusedParameters")
    public void receive(TransportChannel channel, Streamable event) {
        receive(event);
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

    public RaftMetadata currentMeta() {
        return state.currentMetadata;
    }

    public ReplicatedLog currentLog() {
        return replicatedLog;
    }

    private State stay() {
        return state.stay();
    }

    private State stay(RaftMetadata newMeta) {
        return state.stay(newMeta);
    }

    private State goTo(RaftState newState) {
        return state.goTo(newState);
    }

    private State goTo(RaftState newState, RaftMetadata newMeta) {
        return state.goTo(newState, newMeta);
    }

    // behavior related

    private ReplicatedLog maybeCommitEntry(RaftMetadata meta, LogIndexMap matchIndex, ReplicatedLog replicatedLog) {
        return matchIndex.consensusForIndex(meta.getConfig())
            .filter(consensus -> consensus > replicatedLog.committedIndex())
            .map(indexOnMajority -> {
                logger.debug("consensus for persisted index: {}, committed index: {}", indexOnMajority, replicatedLog.committedIndex());
                ImmutableList<LogEntry> entries = replicatedLog.slice(replicatedLog.committedIndex(), indexOnMajority);
                for (LogEntry entry : entries) {
                    if (entry.getCommand() instanceof JointConsensusClusterConfiguration) {
                        JointConsensusClusterConfiguration config = (JointConsensusClusterConfiguration) entry.getCommand();
                        receive(new ClientMessage(self(), config.transitionToStable()));
                    } else if (entry.getCommand() instanceof StableClusterConfiguration) {
                        logger.info("now on stable configuration");
                    } else {
                        logger.info("committing log at index: {}", entry.getIndex());
                        logger.trace("applying command[index={}]: {}, will send result to client: {}", entry.getIndex(), entry.getCommand(), entry.getClient());
                        Streamable result = resourceFSM.apply(entry.getCommand());
                        if (result != null) {
                            Optional<DiscoveryNode> client = entry.getClient();
                            if (client.isPresent()) {
                                send(client.get(), result);
                            }
                        }
                    }
                }
                return replicatedLog.commit(indexOnMajority);
            })
            .orElse(replicatedLog);
    }

    private RaftMetadata maybeUpdateConfiguration(RaftMetadata meta, Streamable command) {
        if (command instanceof ClusterConfiguration && ((ClusterConfiguration) command).isNewer(meta.getConfig())) {
            logger.info("appended new configuration, will start using it now: {}", command);
            return meta.withConfig((ClusterConfiguration) command);
        } else {
            return meta;
        }
    }

    private void stopHeartbeat() {
        cancelTimer("raft-heartbeat");
    }

    private void startHeartbeat(RaftMetadata meta) {
        sendHeartbeat(meta);
        logger.info("starting heartbeat");
        setTimer("raft-heartbeat", SendHeartbeat.INSTANCE, heartbeat, TimeUnit.MILLISECONDS, true);
    }

    private void sendHeartbeat(RaftMetadata meta) {
        for (DiscoveryNode member : meta.membersWithout(self())) {
            sendEntries(member, meta);
        }
    }

    private void sendEntries(DiscoveryNode follower, RaftMetadata meta) {
        long lastIndex = nextIndex.indexFor(follower).orElse(nextIndexDefault);

        if (replicatedLog.hasSnapshot()) {
            RaftSnapshot snapshot = replicatedLog.snapshot();
            if (snapshot.getMeta().getLastIncludedIndex() >= lastIndex) {
                logger.info("send install snapshot to {} in term {}", follower, meta.getCurrentTerm());
                send(follower, new InstallSnapshot(self(), meta.getCurrentTerm(), snapshot));
                return;
            }
        }

        logger.info("send heartbeat to {} in term {} from index {}", follower, meta.getCurrentTerm(), lastIndex);
        send(follower, appendEntries(
            meta.getCurrentTerm(),
            replicatedLog,
            lastIndex,
            replicatedLog.committedIndex()
        ));
    }

    private AppendEntries appendEntries(Term term, ReplicatedLog replicatedLog, long lastIndex, long leaderCommitIdx) {
        if (lastIndex > replicatedLog.nextIndex()) {
            throw new Error("Unexpected from index " + lastIndex + " > " + replicatedLog.nextIndex());
        } else {
            ImmutableList<LogEntry> entries = replicatedLog.entriesBatchFrom(lastIndex);
            long prevIndex = Math.max(0, lastIndex - 1);
            Term prevTerm = replicatedLog.termAt(prevIndex);
            logger.debug("send append entries[{}] term:{} from index:{} to {}", entries.size(), term, lastIndex);
            return new AppendEntries(self(), term, prevTerm, prevIndex, entries, leaderCommitIdx);
        }
    }

    private void initializeLeaderState() {
        nextIndex = new LogIndexMap();
        nextIndexDefault = replicatedLog.lastIndex() + 1;
        matchIndex = new LogIndexMap();
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

    private State beginElection(RaftMetadata meta) {
        resetElectionDeadline();
        if (meta.getConfig().members().isEmpty()) {
            return goTo(Follower);
        } else {
            return goTo(Candidate, meta.forNewElection());
        }
    }

    private DiscoveryNode self() {
        return transportServer.localNode();
    }

    private void send(DiscoveryNode node, Streamable message) {
        if (node.equals(self())) {
            receive(message);
        } else {
            transportService.connectToNode(node);
            transportService.channel(node).send(new MessageTransportFrame(Version.CURRENT, message));
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private State appendEntries(AppendEntries msg, RaftMetadata meta) {
        if (leaderIsLagging(msg, meta)) {
            logger.info("rejecting write (Leader is lagging) of: {} {}", msg, replicatedLog);
            send(msg.getMember(), new AppendRejected(self(), meta.getCurrentTerm()));
            return stay();
        }

        senderIsCurrentLeader(msg.getMember());
        if (!msg.getEntries().isEmpty()) {
            long atIndex = msg.getEntries().get(0).getIndex();
            logger.debug("append({}, {}) to {}", msg.getEntries(), atIndex - 1, replicatedLog);
            replicatedLog = replicatedLog.append(msg.getEntries(), atIndex - 1);
        }
        logger.info("response append successful term:{} lastIndex:{}", meta.getCurrentTerm(), replicatedLog.lastIndex());
        AppendSuccessful response = new AppendSuccessful(self(), meta.getCurrentTerm(), replicatedLog.lastIndex());
        send(msg.getMember(), response);

        ImmutableList<LogEntry> entries = replicatedLog.slice(replicatedLog.committedIndex(), msg.getLeaderCommit());
        for (LogEntry entry : entries) {
            if (entry.getCommand() instanceof ClusterConfiguration) {
                // ignore
            } else if (entry.getCommand() instanceof Noop) {
                // ignore
            } else if (entry.getCommand() instanceof RaftSnapshot) {
                logger.warn("unexpected raft snapshot in log");
            } else {
                logger.info("committing entry {} on follower, leader is committed until [{}]", entry, msg.getLeaderCommit());
                resourceFSM.apply(entry.getCommand());
            }
            replicatedLog = replicatedLog.commit(entry.getIndex());
        }

        if (replicatedLog.committedEntries() >= snapshotInterval) {
            receive(InitLogSnapshot.INSTANCE);
        }

        ClusterConfiguration config = msg.getEntries().stream()
            .map(LogEntry::getCommand)
            .filter(cmd -> cmd instanceof ClusterConfiguration)
            .map(cmd -> (ClusterConfiguration) cmd)
            .reduce(meta.getConfig(), (a, b) -> b);

        resetElectionDeadline();
        return stay(meta.withTerm(replicatedLog.lastTerm()).withConfig(config));
    }

    private boolean leaderIsLagging(AppendEntries msg, RaftMetadata meta) {
        return msg.getTerm().less(meta.getCurrentTerm());
    }

    private void senderIsCurrentLeader(DiscoveryNode leader) {
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
            when(builder, RaftMemberAdded.class, this::handle);
            when(builder, RaftMemberRemoved.class, this::handle);
            when(builder, RaftMembersDiscoveryTimeout.class, this::handle);
            when(builder, RaftMembersDiscoveryRequest.class, this::handle);
            when(builder, RaftMembersDiscoveryResponse.class, this::handle);
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
            when(builder, ElectionMessage.class, this::handle);
            when(builder, AppendRejected.class, this::handle);
            when(builder, AppendSuccessful.class, this::handle);
            when(builder, InstallSnapshotSuccessful.class, this::handle);
            when(builder, InstallSnapshotRejected.class, this::handle);
            when(builder, RequestConfiguration.class, this::handle);
            when(builder, ApplyCommittedLog.class, this::handle);
            handlerMap = builder.build();
        }

        private <T extends Streamable> void when(
            ImmutableMap.Builder<Class, BehaviorHandler> builder,
            Class<T> messageClass,
            BehaviorHandler<T> handler) {
            builder.put(messageClass, handler);
        }

        // initial behavior
        public State handle(ChangeConfiguration message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(AppendEntries message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(RaftMemberAdded message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(RaftMemberRemoved message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(RaftMembersDiscoveryTimeout message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(RaftMembersDiscoveryRequest message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(RaftMembersDiscoveryResponse message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(InitLogSnapshot message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(ClientMessage message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

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

        public State handle(ElectionMessage message, RaftMetadata meta) {
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

        public State handle(RequestConfiguration message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        public State handle(ApplyCommittedLog message, RaftMetadata meta) {
            logger.warn("unhandled: {} in {}", message, state.currentState);
            return stay();
        }

        @SuppressWarnings("unchecked")
        public State receiveMessage(Streamable event, RaftMetadata currentMetadata) {
            BehaviorHandler handler = handlerMap.get(event.getClass());
            if (handler != null) {
                return handler.handle(event, currentMetadata);
            } else return null;
        }

        public void stash(Streamable streamable) {
            stashed.add(streamable);
        }

        public abstract void unstashAll();
    }

    private abstract class LocalClusterBehavior extends Behavior {
        @Override
        public State handle(RaftMembersDiscoveryTimeout message, RaftMetadata meta) {
            logger.info("discovery timeout");
            for (DiscoveryNode node : clusterDiscovery.getDiscoveryNodes()) {
                if (!node.equals(self())) {
                    try {
                        send(node, new RaftMembersDiscoveryRequest(self()));
                    } catch (Exception e) {
                        logger.warn("error connect to {}", node);
                    }
                }
            }
            setTimer("raft-discovery-timeout", RaftMembersDiscoveryTimeout.INSTANCE, discoveryTimeout, TimeUnit.MILLISECONDS);
            return stay();
        }

        @Override
        public State handle(RaftMembersDiscoveryRequest message, RaftMetadata meta) {
            send(message.getMember(), new RaftMembersDiscoveryResponse(self()));
            return stay();
        }

        @Override
        public State handle(RaftMembersDiscoveryResponse message, RaftMetadata meta) {
            logger.info("adding actor {} to raft cluster", message.getMember());
            receive(new RaftMemberAdded(message.getMember(), keepInitUntilFound));
            return stay();
        }
    }

    private abstract class ClusterManagementBehavior extends LocalClusterBehavior {
        @Override
        public State handle(ChangeConfiguration message, RaftMetadata meta) {
            ClusterConfiguration transitioningConfig = meta.getConfig().transitionTo(message.getNewConf());
            if (!transitioningConfig.transitionToStable().members().equals(meta.getConfig().members())) {
                logger.info("starting transition to new configuration, old [size: {}]: {}, migrating to [size: {}]: {}",
                    meta.getConfig().members().size(), meta.getConfig().members(),
                    transitioningConfig.transitionToStable().members().size(),
                    transitioningConfig
                );
            }
            receive(new ClientMessage(self(), transitioningConfig));
            return stay();
        }
    }

    private abstract class SnapshotBehavior extends ClusterManagementBehavior {
        @Override
        public State handle(InitLogSnapshot message, RaftMetadata meta) {
            long committedIndex = replicatedLog.committedIndex();
            RaftSnapshotMetadata snapshotMeta = new RaftSnapshotMetadata(replicatedLog.termAt(committedIndex), committedIndex, meta.getConfig());
            logger.info("init snapshot up to: {}:{}", snapshotMeta.getLastIncludedIndex(), snapshotMeta.getLastIncludedTerm());

            CompletableFuture<Optional<RaftSnapshot>> snapshotFuture = resourceFSM.prepareSnapshot(snapshotMeta);

            snapshotFuture.whenComplete((raftSnapshot, error) -> {
                if (error == null) {
                    if (raftSnapshot.isPresent()) {
                        logger.info("successfully prepared snapshot for {}:{}, compacting log now", snapshotMeta.getLastIncludedIndex(), snapshotMeta.getLastIncludedTerm());
                        replicatedLog = replicatedLog.compactedWith(raftSnapshot.get());
                    } else {
                        logger.info("no snapshot data obtained");
                    }
                } else {
                    logger.error("unable to prepare snapshot!", error);
                }
            });

            return stay();
        }
    }

    private class InitialBehavior extends LocalClusterBehavior {
        @Override
        public State handle(ChangeConfiguration message, RaftMetadata meta) {
            logger.info("applying initial raft cluster configuration, consists of [{}] nodes: {}",
                message.getNewConf().members().size(),
                message.getNewConf().members());
            resetElectionDeadline();
            logger.info("finished init of new raft member, becoming follower");
            return goTo(Follower, meta.withConfig(message.getNewConf()));
        }

        @Override
        public State handle(AppendEntries message, RaftMetadata meta) {
            logger.info("cot append entries from a leader, but am in init state, will ask for it's configuration and join raft cluster");
            send(message.getMember(), new RequestConfiguration(self()));
            return goTo(Candidate);
        }

        @Override
        public State handle(RaftMemberAdded message, RaftMetadata meta) {
            logger.info("current members: {}, add: {}", meta.members(), message.getMember());
            ImmutableSet<DiscoveryNode> newMembers = ImmutableSet.<DiscoveryNode>builder()
                .addAll(meta.members())
                .add(message.getMember())
                .build();
            StableClusterConfiguration initialConfig = new StableClusterConfiguration(0, newMembers);
            if (message.getKeepInitUntil() <= newMembers.size()) {
                logger.info("discovered the required min of {} raft cluster members, becoming follower", message.getKeepInitUntil());
                return goTo(Follower, meta.withConfig(initialConfig));
            } else {
                logger.info("up to {} discovered raft cluster members, still waiting in init until {} discovered.", newMembers.size(), message.getKeepInitUntil());
                return stay(meta.withConfig(initialConfig));
            }
        }

        @Override
        public State handle(RaftMemberRemoved message, RaftMetadata meta) {
            ImmutableSet<DiscoveryNode> newMembers = meta.membersWithout(message.getMember());
            StableClusterConfiguration waitingConfig = new StableClusterConfiguration(0, newMembers);
            logger.info("removed one member, until now discovered {} raft cluster members, still waiting in init until {} discovered.", newMembers.size(), message.getKeepInitUntil());
            return stay(meta.withConfig(waitingConfig));
        }

        @Override
        public State handle(ApplyCommittedLog message, RaftMetadata meta) {
            receive(message);
            return goTo(Follower);
        }

        @Override
        public void unstashAll() {
            logger.warn("try call unstash in init state");
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
                if (replicatedLog.lastTerm().filter(term -> message.getLastLogTerm().less(term)).isPresent()) {
                    logger.warn("rejecting vote for {} at term {}, candidate's lastLogTerm: {} < ours: {}",
                        message.getCandidate(),
                        message.getTerm(),
                        message.getLastLogTerm(),
                        replicatedLog.lastTerm());
                    send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                    return stay(meta);
                }
                if (replicatedLog.lastTerm().filter(term -> term.equals(message.getLastLogTerm())).isPresent() &&
                    message.getLastLogIndex() < replicatedLog.lastIndex()) {
                    logger.warn("rejecting vote for {} at term {}, candidate's lastLogIndex: {} < ours: {}",
                        message.getCandidate(),
                        message.getTerm(),
                        message.getLastLogIndex(),
                        replicatedLog.lastIndex());
                    send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                    return stay(meta);
                }

                logger.info("voting for {} in {}", message.getCandidate(), message.getTerm());
                send(message.getCandidate(), new VoteCandidate(self(), meta.getCurrentTerm()));
                return stay(meta.withVoteFor(message.getCandidate()));
            } else if (meta.getVotedFor().isPresent()) {
                logger.warn("rejecting vote for {}, and {}, currentTerm: {}, already voted for: {}",
                    message.getCandidate(),
                    message.getTerm(),
                    meta.getCurrentTerm(),
                    meta.getVotedFor());
                send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                return stay(meta);
            } else {
                logger.warn("rejecting vote for {}, and {}, currentTerm: {}, received stale term number {}",
                    message.getCandidate(), message.getTerm(),
                    meta.getCurrentTerm(), message.getTerm());
                send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                return stay(meta);
            }
        }

        @Override
        public State handle(AppendEntries message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("received newer {}, current term is {}", message.getTerm(), meta.getCurrentTerm());
                meta = meta.withTerm(message.getTerm());
            }
            if (!replicatedLog.containsMatchingEntry(message.getPrevLogTerm(), message.getPrevLogIndex())) {
                logger.warn("rejecting write (inconsistent log): {}:{} {} ", message.getPrevLogTerm(), message.getPrevLogIndex(), replicatedLog);
                send(message.getMember(), new AppendRejected(self(), meta.getCurrentTerm()));
                return stay(meta);
            } else {
                return appendEntries(message, meta);
            }
        }

        @SuppressWarnings("StatementWithEmptyBody")
        @Override
        public State handle(ApplyCommittedLog message, RaftMetadata meta) {
            ImmutableList<LogEntry> committed = replicatedLog.slice(0, replicatedLog.committedIndex());
            for (LogEntry entry : committed) {
                logger.info("committing entry {} on follower, leader is committed until [{}]", entry, replicatedLog.committedIndex());
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
                    replicatedLog = replicatedLog.compactedWith(snapshot);
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
                send(message.getLeader(), new InstallSnapshotRejected(self(), meta.getCurrentTerm()));
                return stay(meta);
            } else {
                resetElectionDeadline();
                logger.info("got snapshot from {}, is for: {}", message.getLeader(), message.getSnapshot().getMeta());

                meta = meta.withConfig(message.getSnapshot().getMeta().getConfig());
                replicatedLog = replicatedLog.compactedWith(message.getSnapshot());
                resourceFSM.apply(message.getSnapshot().getData());

                logger.info("response snapshot installed in {} last index {}", meta.getCurrentTerm(), replicatedLog.lastIndex());
                send(message.getLeader(), new InstallSnapshotSuccessful(self(), meta.getCurrentTerm(), replicatedLog.lastIndex()));

                return stay(meta);
            }
        }

        @Override
        public State handle(ElectionTimeout message, RaftMetadata meta) {
            return beginElection(meta);
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
            if (meta.getConfig().members().isEmpty()) {
                logger.warn("tried to initialize election with no members");
                return goTo(Follower);
            } else {
                logger.info("initializing election (among {} nodes) for {}", meta.getConfig().members().size(), meta.getCurrentTerm());
                RequestVote request = new RequestVote(meta.getCurrentTerm(), self(), replicatedLog.lastTerm().orElseGet(() -> new Term(0)), replicatedLog.lastIndex());
                for (DiscoveryNode member : meta.membersWithout(self())) {
                    send(member, request);
                }
                return stay(meta.incVote().withVoteFor(self()));
            }
        }

        @Override
        public State handle(RequestVote message, RaftMetadata meta) {
            if (message.getTerm().less(meta.getCurrentTerm())) {
                logger.info("rejecting request vote msg by {} in {}, received stale {}.", message.getCandidate(), meta.getCurrentTerm(), message.getTerm());
                send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                return stay();
            }
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(), meta.getCurrentTerm());
                return goTo(Follower, meta.withTerm(message.getTerm()));
            }
            if (meta.canVoteIn(message.getTerm())) {
                logger.info("voting for {} in {}", message.getCandidate(), meta.getCurrentTerm());
                send(message.getCandidate(), new VoteCandidate(self(), meta.getCurrentTerm()));
                return stay(meta.withVoteFor(message.getCandidate()));
            } else {
                logger.info("rejecting requestVote msg by {} in {}, already voted for {}", message.getCandidate(), meta.getCurrentTerm(), meta.getVotedFor());
                send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                return stay();
            }
        }

        @Override
        public State handle(VoteCandidate message, RaftMetadata meta) {
            if (message.getTerm().less(meta.getCurrentTerm())) {
                logger.info("rejecting vote candidate msg by {} in {}, received stale {}.", message.getMember(), meta.getCurrentTerm(), message.getTerm());
                send(message.getMember(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                return stay();
            }
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(), meta.getCurrentTerm());
                return goTo(Follower, meta.withTerm(message.getTerm()));
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
                return goTo(Follower, meta.withTerm(message.getTerm()));
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
                send(self(), message);
                return goTo(Follower);
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
                return goTo(Follower);
            }
        }

        @Override
        public void unstashAll() {
            logger.warn("try unstash in state candidate");
        }
    }

    private class LeaderBehavior extends SnapshotBehavior {

        private void appendStableClusterConfiguration(RaftMetadata meta) {
            LogEntry entry = new LogEntry(meta.getConfig(), meta.getCurrentTerm(), replicatedLog.nextIndex());
            logger.info("adding to log cluster configuration entry: {}", entry);
            replicatedLog = replicatedLog.append(entry);
            matchIndex.put(self(), entry.getIndex());
            logger.info("log status = {}", replicatedLog);
        }

        private void appendNoop(RaftMetadata meta) {
            LogEntry entry = new LogEntry(Noop.INSTANCE, meta.getCurrentTerm(), replicatedLog.nextIndex());
            logger.debug("adding to log noop entry: {}", entry);
            replicatedLog = replicatedLog.append(entry);
            matchIndex.put(self(), entry.getIndex());
            logger.debug("log status = {}", replicatedLog);
        }

        @Override
        public State handle(ElectedAsLeader message, RaftMetadata meta) {
            logger.info("became leader for {}", meta.getCurrentTerm());
            initializeLeaderState();
            if (replicatedLog.entries().isEmpty()) {
                appendStableClusterConfiguration(meta);
            } else {
                appendNoop(meta);
            }
            startHeartbeat(meta);
            unstashAll();
            return stay();
        }

        @Override
        public State handle(SendHeartbeat message, RaftMetadata meta) {
            sendHeartbeat(meta);
            return stay();
        }

        @Override
        public State handle(ElectionMessage message, RaftMetadata meta) {
            logger.info("got election message");
            return stay();
        }

        @Override
        public State handle(ClientMessage message, RaftMetadata meta) {
            logger.info("appending command: [{}] from {} to replicated log", message.getCmd(), message.getClient());

            LogEntry entry = new LogEntry(message.getCmd(), meta.getCurrentTerm(), replicatedLog.nextIndex(), Optional.of(message.getClient()));

            logger.info("adding to log: {}", entry);
            replicatedLog = replicatedLog.append(entry);
            matchIndex.put(self(), entry.getIndex());

            logger.info("log status = {}", replicatedLog);

            RaftMetadata updated = maybeUpdateConfiguration(meta, entry.getCommand());
            if (updated.getConfig().containsOnNewState(self())) {
                return stay(updated);
            } else {
                return goTo(Follower, updated);
            }
        }

        @Override
        public State handle(AppendEntries message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("leader ({}) got append entries from fresher leader ({}), will step down and the leader will keep being: {}", meta.getCurrentTerm(), message.getTerm(), message.getMember());
                return goTo(Follower);
            } else {
                logger.warn("leader ({}) got append entries from rogue leader ({} @ {}), it's not fresher than self, will send entries, to force it to step down.", meta.getCurrentTerm(), message.getMember(), message.getTerm());
                sendEntries(message.getMember(), meta);
                return stay();
            }
        }

        @Override
        public State handle(AppendRejected message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                return goTo(Follower, meta.withTerm(message.getTerm()));
            }
            if (message.getTerm().equals(meta.getCurrentTerm())) {
                if (!nextIndex.indexFor(message.getMember()).isPresent()) {
                    nextIndex.put(message.getMember(), nextIndexDefault);
                }
                if (nextIndex.indexFor(message.getMember()).filter(index -> index > 1).isPresent()) {
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
            if (message.getTerm().equals(meta.getCurrentTerm())) {
                logger.info("received append successful {} in term: {}", message, meta.getCurrentTerm());
                assert (message.getLastIndex() <= replicatedLog.lastIndex());
                if (message.getLastIndex() > 0) {
                    nextIndex.put(message.getMember(), message.getLastIndex() + 1);
                }
                matchIndex.putIfGreater(message.getMember(), message.getLastIndex());
                replicatedLog = maybeCommitEntry(meta, matchIndex, replicatedLog);
                if (replicatedLog.committedEntries() >= snapshotInterval) {
                    receive(InitLogSnapshot.INSTANCE);
                }
                return stay();
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
                send(self(), message);
                return goTo(Follower, meta.withTerm(message.getTerm()));
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
            if (message.getTerm().equals(meta.getCurrentTerm())) {
                assert (message.getLastIndex() <= replicatedLog.lastIndex());
                if (message.getLastIndex() > 0) {
                    nextIndex.put(message.getMember(), message.getLastIndex() + 1);
                }
                matchIndex.putIfGreater(message.getMember(), message.getLastIndex());
                replicatedLog = maybeCommitEntry(meta, matchIndex, replicatedLog);
                return stay();
            } else {
                logger.warn("unexpected install snapshot successful: {} in term:{}", message, meta.getCurrentTerm());
                return stay();
            }
        }

        @Override
        public State handle(InstallSnapshotRejected message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                // since there seems to be another leader!
                return goTo(Follower, meta.withTerm(message.getTerm()));
            } else if (message.getTerm().equals(meta.getCurrentTerm())) {
                logger.info("follower {} rejected write: {}, back out the first index in this term and retry", message.getMember(), message.getTerm());
                if (!nextIndex.indexFor(message.getMember()).isPresent()) {
                    nextIndex.put(message.getMember(), nextIndexDefault);
                }
                if (nextIndex.indexFor(message.getMember()).filter(index -> index > 1).isPresent()) {
                    nextIndex.decrementFor(message.getMember());
                }
                return stay();
            } else {
                logger.warn("unexpected install snapshot successful: {} in term:{}", message, meta.getCurrentTerm());
                return stay();
            }
        }

        @Override
        public State handle(RequestConfiguration message, RaftMetadata meta) {
            send(message.getMember(), new ChangeConfiguration(meta.getConfig()));
            return stay();
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

        public State goTo(RaftState newState) {
            return new State(newState, currentMetadata);
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
