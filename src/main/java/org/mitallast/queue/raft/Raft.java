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
import org.mitallast.queue.raft.domain.*;
import org.mitallast.queue.raft.log.LogIndexMap;
import org.mitallast.queue.raft.log.ReplicatedLog;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.TransportServer;
import org.mitallast.queue.transport.TransportService;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.mitallast.queue.raft.RaftState.*;

public class Raft extends AbstractLifecycleComponent {

    private final TransportService transportService;
    private final TransportServer transportServer;
    private final ClusterDiscovery clusterDiscovery;

    private Optional<DiscoveryNode> recentlyContactedByLeader;

    private ReplicatedLog replicatedLog = new ReplicatedLog();
    private LogIndexMap nextIndex = new LogIndexMap();
    private LogIndexMap matchIndex = new LogIndexMap();

    private final int keepInitUntilFound;
    private final long electionDeadline;
    private final long discoveryTimeout;
    private final long heartbeat;

    private long nextIndexDefault = 0;

    private final DefaultEventExecutor context;
    private final ConcurrentMap<String, ScheduledFuture> timerMap = new ConcurrentHashMap<>();

    private final ImmutableMap<RaftState, Behavior> stateMap;
    private State state;

    @Inject
    public Raft(
        Config config,
        TransportService transportService,
        TransportServer transportServer,
        ClusterDiscovery clusterDiscovery
    ) {
        super(config.getConfig("raft"), Raft.class);
        this.transportService = transportService;
        this.transportServer = transportServer;
        this.clusterDiscovery = clusterDiscovery;

        recentlyContactedByLeader = Optional.empty();
        replicatedLog = new ReplicatedLog();
        nextIndex = new LogIndexMap();
        matchIndex = new LogIndexMap();

        keepInitUntilFound = this.config.getInt("keep-init-until-found");
        electionDeadline = this.config.getDuration("election-deadline", TimeUnit.MILLISECONDS);
        discoveryTimeout = this.config.getDuration("discovery-timeout", TimeUnit.MILLISECONDS);
        heartbeat = this.config.getDuration("heartbeat", TimeUnit.MILLISECONDS);

        context = new DefaultEventExecutor(new DefaultThreadFactory("raft", true, Thread.MAX_PRIORITY));

        stateMap = ImmutableMap.<RaftState, Behavior>builder()
            .put(Init, new InitialBehavior())
            .put(Follower, new FollowerBehavior())
            .put(Candidate, new CandidateBehavior())
            .put(Leader, new LeaderBehavior())
            .build();

        state = new State(Init, new RaftMetadata(), new SelfSender());
    }

    @Override
    protected void doStart() {
        receive(RaftMembersDiscoveryTimeout.INSTANCE);
        receive(new RaftMemberAdded(self(), keepInitUntilFound));
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

    public void receive(TransportChannel channel, Streamable event) {
        receive(new TransportSender(channel), event);
    }

    public void receive(Streamable event) {
        receive(new SelfSender(), event);
    }

    public void receive(Sender sender, Streamable event) {
        context.execute(() -> {
            try {
                State prevState = state;
                state = state.withSender(sender);
                State newState = stateMap.get(state.currentState).receiveMessage(event, state.currentMetadata);
                if (newState == null) {
                    logger.warn("unhandled event: {} in state {}", event, state.currentState);
                } else {
                    state = newState;
                    if (!state.currentState.equals(prevState.currentState)) {
                        logger.info("transition {} to {}", prevState.currentState, state.currentState);
                        onTransition(prevState.currentState, state.currentState);
                    }
                }
            } catch (Exception e) {
                logger.error("unexpected error in fsm", e);
            }
        });
    }

    private Sender sender() {
        return state.sender;
    }

    public RaftMetadata currentMeta() {
        return state.currentMetadata;
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
                logger.info("consensus for persisted index: {}, committed index: {}", indexOnMajority, replicatedLog.committedIndex());
                ImmutableList<LogEntry> entries = replicatedLog.slice(replicatedLog.committedIndex(), indexOnMajority);
                for (LogEntry entry : entries) {
                    if (entry.getCommand() instanceof JointConsensusClusterConfiguration) {
                        JointConsensusClusterConfiguration config = (JointConsensusClusterConfiguration) entry.getCommand();
                        receive(new ClientMessage(self(), config.transitionToStable()));
                    } else if (entry.getCommand() instanceof StableClusterConfiguration) {
                        logger.info("now on stable configuration");
                    } else {
                        logger.info("committing log at index: {}", entry.getIndex());
                        logger.info("applying command[index={}]: {}, will send result to client: {}", entry.getIndex(), entry.getCommand(), entry.getClient());
                        Streamable result = apply(entry.getCommand());
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

        logger.info("send heartbeat to {} in term {}", follower, meta.getCurrentTerm());
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
            logger.info("send append entries[{}] term:{} from index:{}", entries.size(), term, lastIndex);
            return new AppendEntries(self(), term, prevTerm, prevIndex, entries, leaderCommitIdx);
        }
    }

    private void initializeLeaderState() {
        nextIndex = new LogIndexMap();
        nextIndexDefault = replicatedLog.lastIndex();
        matchIndex = new LogIndexMap();
        logger.info("prepare next index and match index table for followers: next index:{}", nextIndexDefault);
    }

    private RaftMetadata apply(DomainEvent domainEvent, RaftMetadata meta) {
        if (domainEvent instanceof UpdateTermEvent) {
            return meta.withTerm(((UpdateTermEvent) domainEvent).getTerm());
        } else if (domainEvent instanceof GoToFollowerEvent) {
            return ((GoToFollowerEvent) domainEvent).getTerm()
                .map(meta::forFollower)
                .orElseGet(meta::forFollower);
        } else if (domainEvent instanceof GoToLeaderEvent) {
            return meta.forLeader();
        } else if (domainEvent instanceof StartElectionEvent) {
            return meta.forNewElection();
        } else if (domainEvent instanceof KeepStateEvent) {
            return meta;
        } else if (domainEvent instanceof VoteForEvent) {
            return meta.withVoteFor(((VoteForEvent) domainEvent).getCandidate());
        } else if (domainEvent instanceof IncrementVoteEvent) {
            return meta.incVote();
        } else if (domainEvent instanceof VoteForSelfEvent) {
            return meta.incVote().withVoteFor(self());
        } else if (domainEvent instanceof WithNewConfigEvent) {
            WithNewConfigEvent event = (WithNewConfigEvent) domainEvent;
            return event.getTerm()
                .map(term -> meta.withConfig(event.getConfig()).withTerm(term))
                .orElseGet(() -> meta.withConfig(event.getConfig()));
        } else {
            throw new IllegalArgumentException("Unexpected domain event: " + domainEvent);
        }
    }

    private Streamable apply(Streamable message) {
        logger.info("apply: {}", message);
        return null;
    }

    private void cancelElectionDeadline() {
        cancelTimer("raft-election-timeout");
    }

    private void resetElectionDeadline() {
        cancelElectionDeadline();
        long timeout = new Random().nextInt((int) (electionDeadline / 2)) + electionDeadline;
        setTimer("raft-election-timeout", ElectionTimeout.INSTANCE, timeout, TimeUnit.MILLISECONDS);
    }

    private State beginElection(RaftMetadata meta) {
        resetElectionDeadline();
        if (meta.getConfig().members().isEmpty()) {
            return goTo(Follower, apply(new KeepStateEvent(), meta));
        } else {
            return goTo(Candidate, apply(new StartElectionEvent(), meta));
        }
    }

    private State stepDown(RaftMetadata meta) {
        return stepDown(meta, Optional.empty());
    }

    private State stepDown(RaftMetadata meta, Optional<Term> term) {
        return goTo(Follower, apply(new GoToFollowerEvent(term), meta));
    }

    private State acceptHeartbeat() {
        resetElectionDeadline();
        return stay();
    }

    private DiscoveryNode self() {
        return transportServer.localNode();
    }

    private void forward(DiscoveryNode node, Streamable message) {
        transportService.channel(node).send(new MessageTransportFrame(Version.CURRENT, message));
    }

    private void send(DiscoveryNode node, Streamable message) {
        if (node.equals(self())) {
            receive(message);
        } else {
            transportService.channel(node).send(new MessageTransportFrame(Version.CURRENT, message));
        }
    }

    @SuppressWarnings("UnusedParameters")
    private CompletableFuture<Optional<RaftSnapshot>> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    private State appendEntries(AppendEntries msg, RaftMetadata meta) {
        if (leaderIsLagging(msg, meta)) {
            logger.info("rejecting write (Leader is lagging) of: {} {}", msg, replicatedLog);
            send(msg.getMember(), new AppendRejected(self(), meta.getCurrentTerm()));
            return stay();
        }

        senderIsCurrentLeader(msg.getMember());
        send(msg.getMember(), append(msg.getEntries(), meta));
        replicatedLog = commitUntilLeadersIndex(meta, msg);

        ClusterConfiguration config = maybeGetNewConfiguration(msg.getEntries().stream().map(LogEntry::getCommand).collect(Collectors.toList()), meta.getConfig());

        return acceptHeartbeat().stay(apply(new WithNewConfigEvent(replicatedLog.lastTerm(), config), meta));
    }

    @SuppressWarnings({"StatementWithEmptyBody", "UnusedParameters"})
    private ReplicatedLog commitUntilLeadersIndex(RaftMetadata meta, AppendEntries msg) {
        ImmutableList<LogEntry> entries = replicatedLog.slice(replicatedLog.committedIndex(), msg.getLeaderCommit());

        ReplicatedLog log = replicatedLog;
        for (LogEntry entry : entries) {
            logger.info("committing entry {} on follower, leader is committed until [{}]", entry, msg.getLeaderCommit());
            if (entry.getCommand() instanceof ClusterConfiguration) {
                // ignore
            } else {
                apply(entry);
            }
            log = log.commit(entry.getIndex());
        }
        return log;
    }

    private AppendSuccessful append(ImmutableList<LogEntry> entries, RaftMetadata meta) {
        if (!entries.isEmpty()) {
            long atIndex = entries.get(0).getIndex();
            for (LogEntry entry : entries) {
                atIndex = Math.min(atIndex, entry.getIndex());
            }
            logger.info("executing: replicatedLog = replicatedLog.append({}, {})", entries, atIndex - 1);
            replicatedLog = replicatedLog.append(entries, atIndex - 1);
        }
        logger.info("response append successful term:{} lastIndex:{}", meta.getCurrentTerm(), replicatedLog.lastIndex());
        return new AppendSuccessful(self(), meta.getCurrentTerm(), replicatedLog.lastIndex());
    }

    private ClusterConfiguration maybeGetNewConfiguration(List<Streamable> entries, ClusterConfiguration config) {
        if (entries.isEmpty()) {
            return config;
        }
        for (Streamable entry : entries) {
            if (entry instanceof ClusterConfiguration) {
                ClusterConfiguration newConfig = (ClusterConfiguration) entry;
                logger.info("appended new configuration (seq: {}), will start using it now: {}", newConfig.sequenceNumber(), newConfig);
                return newConfig;
            }
        }
        return config;
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
            when(builder, WhoIsTheLeader.class, this::handle);
            when(builder, RaftMembersDiscoveryTimeout.class, this::handle);
            when(builder, RaftMembersDiscoveryRequest.class, this::handle);
            when(builder, RaftMembersDiscoveryResponse.class, this::handle);
            when(builder, InitLogSnapshot.class, this::handle);
            when(builder, ClientMessage.class, this::handle);
            when(builder, RequestVote.class, this::handle);
            when(builder, InstallSnapshot.class, this::handle);
            when(builder, ElectionTimeout.class, this::handle);
            when(builder, AskForState.class, this::handle);
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

        public State handle(WhoIsTheLeader message, RaftMetadata meta) {
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

        public State handle(AskForState message, RaftMetadata meta) {
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

        @SuppressWarnings("unchecked")
        public State receiveMessage(Streamable event, RaftMetadata currentMetadata) {
            return handlerMap.get(event.getClass()).handle(event, currentMetadata);
        }
    }

    private abstract class LocalClusterBehavior extends Behavior {
        @Override
        public State handle(RaftMembersDiscoveryTimeout message, RaftMetadata meta) {
            logger.info("discovery timeout");
            for (DiscoveryNode node : clusterDiscovery.getDiscoveryNodes()) {
                if (!node.equals(self())) {
                    try {
                        transportService.connectToNode(node);
                        transportService.channel(node).send(new MessageTransportFrame(Version.CURRENT, RaftMembersDiscoveryRequest.INSTANCE));
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
            sender().send(new RaftMembersDiscoveryResponse(self()));
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
        public State handle(WhoIsTheLeader message, RaftMetadata meta) {
            sender().send(new LeaderIs(Optional.empty(), Optional.empty()));
            return stay();
        }

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

            CompletableFuture<Optional<RaftSnapshot>> snapshotFuture = prepareSnapshot(snapshotMeta);

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
            return goTo(Follower, apply(new WithNewConfigEvent(meta.getConfig()), meta));
        }

        @Override
        public State handle(AppendEntries message, RaftMetadata meta) {
            logger.info("cot append entries from a leader, but am in init state, will ask for it's configuration and join raft cluster");
            send(message.getMember(), RequestConfiguration.INSTANCE);
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
                return goTo(Follower, apply(new WithNewConfigEvent(initialConfig), meta));
            } else {
                logger.info("up to {} discovered raft cluster members, still waiting in init until {} discovered.", newMembers.size(), message.getKeepInitUntil());
                return stay(apply(new WithNewConfigEvent(initialConfig), meta));
            }
        }

        @Override
        public State handle(RaftMemberRemoved message, RaftMetadata meta) {
            ImmutableSet<DiscoveryNode> newMembers = meta.membersWithout(message.getMember());
            StableClusterConfiguration waitingConfig = new StableClusterConfiguration(0, newMembers);
            logger.info("removed one member, until now discovered {} raft cluster members, still waiting in init until {} discovered.", newMembers.size(), message.getKeepInitUntil());
            return stay(apply(new WithNewConfigEvent(waitingConfig), meta));
        }
    }

    private class FollowerBehavior extends SnapshotBehavior {
        @Override
        public State handle(ClientMessage message, RaftMetadata meta) {
            logger.info("follower got {} from client, respond with last Leader that took write from: {}", message, recentlyContactedByLeader);
            sender().send(new LeaderIs(recentlyContactedByLeader, Optional.of(message)));
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
                if (replicatedLog.lastTerm().filter(term -> term.equals(message.getLastLogTerm())).isPresent()) {
                    logger.warn("rejecting vote for {}, and {}, candidate's lastLogTerm: {} < ours: {}",
                        message.getCandidate(),
                        message.getTerm(),
                        message.getLastLogTerm(),
                        message.getLastLogTerm());
                    send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                    return stay();
                } else if (replicatedLog.lastTerm().filter(term -> term.equals(message.getLastLogTerm())).isPresent() &&
                    message.getLastLogIndex() < replicatedLog.lastIndex()) {
                    logger.warn("rejecting vote for {}, and {}, candidate's lastLogIndex: {} < ours: {}",
                        message.getCandidate(),
                        message.getTerm(),
                        message.getLastLogIndex(),
                        replicatedLog.lastIndex());
                    send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                    return stay();
                } else {
                    logger.info("voting for {} in {}", message.getCandidate(), message.getTerm());
                    send(message.getCandidate(), new VoteCandidate(self(), meta.getCurrentTerm()));
                    return stay(apply(new VoteForEvent(message.getCandidate()), meta));
                }
            } else if (meta.canVoteIn(message.getTerm())) {
                resetElectionDeadline();
                if (replicatedLog.lastTerm().filter(term -> message.getLastLogTerm().less(term)).isPresent()) {
                    logger.warn("rejecting vote for {}, and {}, candidate's lastLogTerm: {} < ours: {}",
                        message.getCandidate(), message.getTerm(), message.getLastLogTerm(), replicatedLog.lastTerm());
                    send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                    return stay();
                } else if (replicatedLog.lastTerm().filter(term -> term.equals(message.getLastLogTerm())).isPresent() &&
                    message.getLastLogIndex() < replicatedLog.lastIndex()) {
                    logger.warn("rejecting vote for {}, and {}, candidate's lastLogIndex: {} < ours: {}",
                        message.getCandidate(), message.getTerm(), message.getLastLogIndex(), replicatedLog.lastIndex());
                    send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                    return stay();
                } else {
                    logger.info("voting for {} in {}", message.getCandidate(), message.getTerm());
                    send(message.getCandidate(), new VoteCandidate(self(), meta.getCurrentTerm()));
                    return stay(apply(new VoteForEvent(message.getCandidate()), meta));
                }
            } else if (meta.getVotedFor().isPresent()) {
                logger.warn("rejecting vote for {}, and {}, currentTerm: {}, already voted for: {}",
                    message.getCandidate(),
                    message.getTerm(),
                    meta.getCurrentTerm(),
                    meta.getVotedFor());
                send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                return stay();
            } else {
                logger.warn("rejecting vote for {}, and {}, currentTerm: {}, received stale term number {}",
                    message.getCandidate(), message.getTerm(),
                    meta.getCurrentTerm(), message.getTerm());
                send(message.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                return stay();
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
                return stay();
            } else {
                return appendEntries(message, meta);
            }
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
                return stay();
            } else {
                resetElectionDeadline();
                logger.info("got snapshot from {}, is for: {}", message.getLeader(), message.getSnapshot().getMeta());

                apply(message);
                replicatedLog = replicatedLog.compactedWith(message.getSnapshot());

                logger.info("response snapshot installed in {} last index {}", meta.getCurrentTerm(), replicatedLog.lastIndex());
                send(message.getLeader(), new InstallSnapshotSuccessful(self(), meta.getCurrentTerm(), replicatedLog.lastIndex()));

                return stay();
            }
        }

        @Override
        public State handle(ElectionTimeout message, RaftMetadata meta) {
            return beginElection(meta);
        }

        @Override
        public State handle(AskForState message, RaftMetadata meta) {
            sender().send(new IAmInState(Follower));
            return stay();
        }
    }

    private class CandidateBehavior extends SnapshotBehavior {
        @Override
        public State handle(ClientMessage message, RaftMetadata meta) {
            logger.info("candidate got {} from client, respond with anarchy - there is no leader", message);
            sender().send(new LeaderIs(Optional.empty(), Optional.of(message)));
            return stay();
        }

        @Override
        public State handle(BeginElection message, RaftMetadata meta) {
            if (meta.getConfig().members().isEmpty()) {
                logger.warn("tried to initialize election with no members");
                return stepDown(meta);
            } else {
                logger.info("initializing election (among {} nodes) for {}", meta.getConfig().members().size(), meta.getCurrentTerm());
                RequestVote request = new RequestVote(meta.getCurrentTerm(), self(), replicatedLog.lastTerm().orElseGet(() -> new Term(0)), replicatedLog.lastIndex());
                for (DiscoveryNode member : meta.membersWithout(self())) {
                    send(member, request);
                }
                return stay(apply(new VoteForSelfEvent(), meta));
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
                return stepDown(meta, Optional.of(message.getTerm()));
            }
            if (meta.canVoteIn(message.getTerm())) {
                logger.info("voting for {} in {}", message.getCandidate(), meta.getCurrentTerm());
                send(message.getCandidate(), new VoteCandidate(self(), meta.getCurrentTerm()));
                return stay(apply(new VoteForEvent(message.getCandidate()), meta));
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
                return stepDown(meta, Optional.of(message.getTerm()));
            }

            int votesReceived = meta.getVotesReceived() + 1;

            boolean hasWonElection = votesReceived > meta.getConfig().members().size() / 2;
            if (hasWonElection) {
                logger.info("received vote by {}, won election with {} of {} votes", message.getMember(), votesReceived, meta.getConfig().members().size());
                return goTo(Leader, apply(new GoToLeaderEvent(), meta));
            } else {
                logger.info("received vote by {}, have {} of {} votes", message.getMember(), votesReceived, meta.getConfig().members().size());
                return stay(apply(new IncrementVoteEvent(), meta));
            }
        }

        @Override
        public State handle(DeclineCandidate message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(), meta.getCurrentTerm());
                return stepDown(meta, Optional.of(message.getTerm()));
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
                forward(self(), message);
                return stepDown(meta);
            } else {
                return stay();
            }
        }

        @Override
        public State handle(ElectionTimeout message, RaftMetadata meta) {
            if (meta.getConfig().members().size() > 1) {
                logger.info("voting timeout, starting a new election (among {})", meta.getConfig().members().size());
                receive(BeginElection.INSTANCE);
                return stay(apply(new StartElectionEvent(), meta));
            } else {
                logger.info("voting timeout, unable to start election, don't know enough nodes (members: {})...", meta.getConfig().members().size());
                return stepDown(meta);
            }
        }

        @Override
        public State handle(AskForState message, RaftMetadata meta) {
            sender().send(new IAmInState(Candidate));
            return stay();
        }
    }

    private class LeaderBehavior extends SnapshotBehavior {
        @Override
        public State handle(ElectedAsLeader message, RaftMetadata meta) {
            logger.info("became leader for {}", meta.getCurrentTerm());
            initializeLeaderState();
            startHeartbeat(meta);
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
                return stay(apply(new KeepStateEvent(), updated));
            } else {
                return stepDown(updated);
            }
        }

        @Override
        public State handle(AppendEntries message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                logger.info("leader ({}) got append entries from fresher leader ({}), will step down and the leader will keep being: {}", meta.getCurrentTerm(), message.getTerm(), message.getMember());
                return stepDown(meta);
            } else {
                logger.warn("leader ({}) got append entries from rogue leader ({} @ {}), it's not fresher than self, will send entries, to force it to step down.", meta.getCurrentTerm(), message.getMember(), message.getTerm());
                sendEntries(message.getMember(), meta);
                return stay();
            }
        }

        @Override
        public State handle(AppendRejected message, RaftMetadata meta) {
            if (message.getTerm().greater(meta.getCurrentTerm())) {
                return stepDown(meta, Optional.of(message.getTerm()));
            }
            if (message.getTerm().equals(meta.getCurrentTerm())) {
                logger.info("follower {} rejected write: {}, back out the first index in this term and retry", message.getMember(), message.getTerm());
                if (nextIndex.indexFor(message.getMember()).filter(index -> index > 1).isPresent()) {
                    nextIndex.decrementFor(message.getMember());
                }
                return stay();
            } else {
                logger.info("follower {} rejected write: {}, ignore", message.getMember(), message.getTerm());
                return stay();
            }
        }

        @Override
        public State handle(AppendSuccessful message, RaftMetadata meta) {
            if (message.getTerm().equals(meta.getCurrentTerm())) {
                assert (message.getLastIndex() <= replicatedLog.lastIndex());
                if (message.getLastIndex() > 0) {
                    nextIndex.put(message.getMember(), message.getLastIndex() + 1);
                }
                matchIndex.putIfGreater(message.getMember(), message.getLastIndex());
                replicatedLog = maybeCommitEntry(meta, matchIndex, replicatedLog);
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
                forward(self(), message);
                return stepDown(meta, Optional.of(message.getTerm()));
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
                return stepDown(meta, Optional.of(message.getTerm()));
            } else if (message.getTerm().equals(meta.getCurrentTerm())) {
                logger.info("follower {} rejected write: {}, back out the first index in this term and retry", message.getMember(), message.getTerm());
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
            sender().send(new ChangeConfiguration(meta.getConfig()));
            return stay();
        }

        @Override
        public State handle(AskForState message, RaftMetadata meta) {
            sender().send(new IAmInState(Leader));
            return stay();
        }
    }

    private class State {
        private final RaftState currentState;
        private final RaftMetadata currentMetadata;
        private final Sender sender;

        private State(RaftState currentState, RaftMetadata currentMetadata, Sender sender) {
            this.currentState = currentState;
            this.currentMetadata = currentMetadata;
            this.sender = sender;
        }

        public State withSender(Sender sender) {
            return new State(currentState, currentMetadata, sender);
        }

        public State stay() {
            return this;
        }

        public State stay(RaftMetadata newMetadata) {
            return new State(currentState, newMetadata, sender);
        }

        public State goTo(RaftState newState) {
            return new State(newState, currentMetadata, sender);
        }

        public State goTo(RaftState newState, RaftMetadata newMetadata) {
            return new State(newState, newMetadata, sender);
        }

        @Override
        public String toString() {
            return "State{" +
                "currentState=" + currentState +
                ", currentMetadata=" + currentMetadata +
                '}';
        }
    }

    private interface Sender {
        void send(Streamable entry);
    }

    private class SelfSender implements Sender {

        @Override
        public void send(Streamable entry) {
            receive(this, entry);
        }
    }

    private class TransportSender implements Sender {
        private final TransportChannel channel;

        private TransportSender(TransportChannel channel) {
            this.channel = channel;
        }

        @Override
        public void send(Streamable entry) {
            channel.send(new MessageTransportFrame(Version.CURRENT, entry));
        }
    }
}
