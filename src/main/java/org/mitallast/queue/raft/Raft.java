package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.typesafe.config.Config;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.proto.ProtoService;
import org.mitallast.queue.proto.raft.*;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.persistent.PersistentService;
import org.mitallast.queue.raft.persistent.ReplicatedLog;
import org.mitallast.queue.raft.resource.ResourceRegistry;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;
import org.slf4j.MDC;

import java.io.IOError;
import java.io.IOException;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.google.protobuf.TextFormat.shortDebugString;
import static org.mitallast.queue.raft.RaftState.*;

public class Raft extends AbstractLifecycleComponent {

    private static final ImmutableMap<Class<? extends Message>, StateConsumer> consumerMap =
        ImmutableMap.<Class<? extends Message>, StateConsumer>builder()
            .put(AppendEntries.class, (state, event) -> state.handle((AppendEntries) event))
            .put(AppendRejected.class, (state, event) -> state.handle((AppendRejected) event))
            .put(AppendSuccessful.class, (state, event) -> state.handle((AppendSuccessful) event))
            .put(RequestVote.class, (state, event) -> state.handle((RequestVote) event))
            .put(VoteCandidate.class, (state, event) -> state.handle((VoteCandidate) event))
            .put(DeclineCandidate.class, (state, event) -> state.handle((DeclineCandidate) event))
            .put(ClientMessage.class, (state, event) -> state.handle((ClientMessage) event))
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
    private final ResourceRegistry registry;
    private final ProtoService protoService;
    private final boolean bootstrap;
    private final long electionDeadline;
    private final long heartbeat;
    private final int snapshotInterval;
    private final int maxEntries;
    private final RaftContext context;
    private final ConcurrentLinkedQueue<ClientMessage> stashed;
    private final ReentrantLock lock;
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
        ResourceRegistry registry,
        ProtoService protoService,
        RaftContext context
    ) throws IOException {
        super(config.getConfig("raft"), Raft.class);
        this.transportService = transportService;
        this.transportController = transportController;
        this.clusterDiscovery = clusterDiscovery;
        this.persistentService = persistentService;
        this.replicatedLog = persistentService.openLog();
        this.registry = registry;
        this.protoService = protoService;
        this.context = context;

        stashed = new ConcurrentLinkedQueue<>();
        lock = new ReentrantLock();
        recentlyContactedByLeader = Optional.empty();
        nextIndex = new LogIndexMap(0);
        matchIndex = new LogIndexMap(0);

        bootstrap = this.config.getBoolean("bootstrap");
        electionDeadline = this.config.getDuration("election-deadline", TimeUnit.MILLISECONDS);
        heartbeat = this.config.getDuration("heartbeat", TimeUnit.MILLISECONDS);
        snapshotInterval = this.config.getInt("snapshot-interval");
        maxEntries = this.config.getInt("max-entries");
    }

    @Override
    protected void doStart() throws IOException {
        RaftMetadata meta = new RaftMetadata(
            persistentService.currentTerm(),
            ClusterConfiguration.newBuilder().setStable(StableClusterConfiguration.newBuilder().build()).build(),
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

    public void apply(Message event) {
        lock.lock();
        try {
            state = state.apply(event);
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

    public ImmutableList<Message> currentStashed() {
        return ImmutableList.copyOf(stashed);
    }

    // behavior related

    private void send(DiscoveryNode node, Message message) {
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

    @FunctionalInterface
    private interface StateConsumer {
        State apply(State state, Message event) throws IOException;
    }

    private abstract class State {
        private RaftMetadata meta;

        private State(RaftMetadata meta) throws IOException {
            this.meta = meta;
            persistentService.updateState(meta.getCurrentTerm(), meta.getVotedFor());
        }

        protected State stay(RaftMetadata meta) throws IOException {
            this.meta = meta;
            persistentService.updateState(meta.getCurrentTerm(), meta.getVotedFor());
            return this;
        }

        public abstract RaftState state();

        public RaftMetadata meta() {
            return meta;
        }

        // replication

        public State apply(Message message) throws IOException {
            try (MDC.MDCCloseable ignore = MDC.putCloseable("state", state().name())) {
                return consumerMap.get(message.getClass()).apply(this, message);
            }
        }

        public abstract State handle(AppendEntries message) throws IOException;

        public State handle(AppendRejected message) throws IOException {
            return unhandled(message);
        }

        public State handle(AppendSuccessful message) throws IOException {
            return unhandled(message);
        }

        // election

        public abstract State handle(RequestVote message) throws IOException;

        public State handle(VoteCandidate message) throws IOException {
            return unhandled(message);
        }

        public State handle(DeclineCandidate message) throws IOException {
            return unhandled(message);
        }

        // leader

        public abstract State handle(ClientMessage message) throws IOException;

        // snapshot

        public State createSnapshot() throws IOException {
            long committedIndex = replicatedLog.committedIndex();
            RaftSnapshotMetadata snapshotMeta = RaftSnapshotMetadata.newBuilder()
                .setLastIncludedTerm(replicatedLog.termAt(committedIndex))
                .setLastIncludedIndex(committedIndex)
                .setConfig(meta.getConfig())
                .build();
            logger.info("init snapshot up to: {}:{}", snapshotMeta.getLastIncludedIndex(), snapshotMeta.getLastIncludedTerm());

            RaftSnapshot snapshot = registry.prepareSnapshot(snapshotMeta);
            logger.info("successfully prepared snapshot for {}:{}, compacting log now", snapshotMeta.getLastIncludedIndex(), snapshotMeta.getLastIncludedTerm());
            replicatedLog.compactWith(snapshot, clusterDiscovery.self());

            return this;
        }

        public abstract State handle(InstallSnapshot message) throws IOException;

        public State handle(InstallSnapshotSuccessful message) throws IOException {
            return unhandled(message);
        }

        public State handle(InstallSnapshotRejected message) throws IOException {
            return unhandled(message);
        }

        // joint consensus

        public abstract State handle(AddServer request) throws IOException;

        public State handle(AddServerResponse message) throws IOException {
            return unhandled(message);
        }

        public abstract State handle(RemoveServer request) throws IOException;

        public State handle(RemoveServerResponse message) throws IOException {
            return unhandled(message);
        }

        // stash messages

        public void stash(ClientMessage message) throws IOException {
            if (logger.isDebugEnabled()) {
                logger.debug("stash {}", message.getDescriptorForType().getFullName());
            }
            stashed.add(message);
        }

        private State unhandled(Message message) throws IOException {
            if (logger.isDebugEnabled()) {
                logger.debug("unhandled: {} in {}", message.getDescriptorForType().getFullName(), state());
            }
            return this;
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

        private State gotoCandidate(RaftMetadata meta) throws IOException {
            resetElectionDeadline();
            return new CandidateState(meta).beginElection();
        }

        private State gotoLeader(RaftMetadata meta) throws IOException {
            context.cancelTimer(RaftContext.ELECTION_TIMEOUT);
            return new LeaderState(meta).elected();
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
                    return handle(AddServer.newBuilder().setMember(clusterDiscovery.self()).build());
                } else {
                    logger.info("joint cluster");
                    return electionTimeout();
                }
            } else {
                ClusterConfiguration config = replicatedLog.entries().stream()
                    .map(LogEntry::getCommand)
                    .filter(cmd -> protoService.is(cmd, ClusterConfiguration.getDescriptor()))
                    .map(cmd -> protoService.unpack(cmd, ClusterConfiguration.parser()))
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
                    send(message.getCandidate(), DeclineCandidate.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta.getCurrentTerm()).build());
                    return stay(meta);
                }
                if (replicatedLog.lastTerm().filter(term -> term.equals(message.getLastLogTerm())).isPresent() &&
                    message.getLastLogIndex() < replicatedLog.lastIndex()) {
                    logger.warn("rejecting vote for {} at term {}, candidate's lastLogIndex: {} < ours: {}",
                        message.getCandidate(),
                        message.getTerm(),
                        message.getLastLogIndex(),
                        replicatedLog.lastIndex());
                    send(message.getCandidate(), DeclineCandidate.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta.getCurrentTerm()).build());
                    return stay(meta);
                }

                logger.info("voting for {} in {}", message.getCandidate(), message.getTerm());
                send(message.getCandidate(), VoteCandidate.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta.getCurrentTerm()).build());
                return stay(meta.withVoteFor(message.getCandidate()));
            } else if (meta.getVotedFor().isPresent()) {
                logger.warn("rejecting vote for {}, and {}, currentTerm: {}, already voted for: {}",
                    message.getCandidate(),
                    message.getTerm(),
                    meta.getCurrentTerm(),
                    meta.getVotedFor());
                send(message.getCandidate(), DeclineCandidate.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta.getCurrentTerm()).build());
                return stay(meta);
            } else {
                logger.warn("rejecting vote for {}, and {}, currentTerm: {}, received stale term number {}",
                    message.getCandidate(), message.getTerm(),
                    meta.getCurrentTerm(), message.getTerm());
                send(message.getCandidate(), DeclineCandidate.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta.getCurrentTerm()).build());
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
                send(message.getMember(), AppendRejected.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta.getCurrentTerm()).setLastIndex(replicatedLog.lastIndex()).build());
                return stay(meta);
            }

            try {
                // 2) Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (5.3)
                if (!replicatedLog.containsMatchingEntry(message.getPrevLogTerm(), message.getPrevLogIndex())) {
                    logger.warn("rejecting write (inconsistent log): {}:{} {} ", message.getPrevLogTerm(), message.getPrevLogIndex(), replicatedLog);
                    send(message.getMember(), AppendRejected.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta.getCurrentTerm()).setLastIndex(replicatedLog.lastIndex()).build());
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

            if (!msg.getEntriesList().isEmpty()) {
                // If an existing entry conflicts with a new one (same index
                // but different terms), delete the existing entry and all that
                // follow it (5.3)

                // Append any new entries not already in the log

                long prevIndex = msg.getEntries(0).getIndex() - 1;
                logger.debug("append({}, {})", msg.getEntriesList(), prevIndex);
                replicatedLog.append(msg.getEntriesList(), prevIndex);
            }
            logger.debug("response append successful term:{} lastIndex:{}", meta.getCurrentTerm(), replicatedLog.lastIndex());
            AppendSuccessful response = AppendSuccessful.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta.getCurrentTerm()).setLastIndex(replicatedLog.lastIndex()).build();
            send(msg.getMember(), response);

            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

            if (msg.getLeaderCommit() > replicatedLog.committedIndex()) {
                ImmutableList<LogEntry> entries = replicatedLog.slice(replicatedLog.committedIndex() + 1, msg.getLeaderCommit());
                for (LogEntry entry : entries) {
                    if (protoService.is(entry.getCommand(), ClusterConfiguration.getDescriptor())) {
                        ClusterConfiguration configuration = protoService.unpack(entry.getCommand(), ClusterConfiguration.parser());
                        logger.info("apply new configuration: {}", configuration);
                        meta = meta.withConfig(configuration);
                    } else if (protoService.is(entry.getCommand(), Noop.getDescriptor())) {
                        logger.trace("ignore noop entry");
                    } else if (protoService.is(entry.getCommand(), RaftSnapshot.getDescriptor())) {
                        logger.warn("unexpected raft snapshot in log");
                    } else {
                        logger.debug("committing entry {} on follower, leader is committed until [{}]", entry, msg.getLeaderCommit());
                        registry.apply(protoService.unpack(entry.getCommand()));
                    }
                    replicatedLog.commit(entry.getIndex());
                }
            }

            ClusterConfiguration config = msg.getEntriesList().stream()
                .map(LogEntry::getCommand)
                .filter(cmd -> protoService.is(cmd, ClusterConfiguration.getDescriptor()))
                .map(cmd -> protoService.unpack(cmd, ClusterConfiguration.parser()))
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
            if (meta().members().isEmpty()) {
                logger.warn("no members found, joint timeout");
                for (DiscoveryNode node : clusterDiscovery.discoveryNodes()) {
                    if (!node.equals(clusterDiscovery.self())) {
                        send(node, AddServer.newBuilder().setMember(clusterDiscovery.self()).build());
                    }
                }
                return this;
            } else {
                return gotoCandidate(meta().forNewElection());
            }
        }

        // joint consensus

        @Override
        public State handle(AddServer request) throws IOException {
            if (bootstrap && meta().members().isEmpty()
                && request.getMember().equals(clusterDiscovery.self())
                && replicatedLog.isEmpty()) {
                logger.info("bootstrap cluster with {}", request.getMember());
                StableClusterConfiguration stable = StableClusterConfiguration.newBuilder().addMembers(request.getMember()).build();
                return gotoLeader(meta()
                    .withTerm(meta().getCurrentTerm() + 1)
                    .withConfig(ClusterConfiguration.newBuilder().setStable(stable).build()));
            }
            AddServerResponse.Builder builder = AddServerResponse.newBuilder().setStatus(AddServerResponse.Status.NOT_LEADER);
            recentlyContactedByLeader.ifPresent(builder::setLeader);
            send(request.getMember(), builder.build());
            return this;
        }

        public State handle(AddServerResponse request) throws IOException {
            if (request.getStatus() == AddServerResponse.Status.OK) {
                logger.info("successful joined");
            }
            DiscoveryNode leader = request.getLeader();
            if (leader.isInitialized()) {
                senderIsCurrentLeader(leader);
                resetElectionDeadline();
            }
            return this;
        }

        @Override
        public State handle(RemoveServer request) throws IOException {
            RemoveServerResponse.Builder builder = RemoveServerResponse.newBuilder().setStatus(RemoveServerResponse.Status.NOT_LEADER);
            recentlyContactedByLeader.ifPresent(builder::setLeader);
            send(request.getMember(), builder.build());
            return this;
        }

        public State handle(RemoveServerResponse request) throws IOException {
            if (request.getStatus() == RemoveServerResponse.Status.OK) {
                logger.info("successful removed");
            }
            if (request.getLeader().isInitialized()) {
                recentlyContactedByLeader = Optional.of(request.getLeader());
            }
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
                logger.info("rejecting install snapshot {}, current term is {}", message.getTerm(), meta.getCurrentTerm());
                send(message.getLeader(), InstallSnapshotRejected.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta.getCurrentTerm()).build());
                return stay(meta);
            } else {
                resetElectionDeadline();
                logger.info("got snapshot from {}, is for: {}", message.getLeader(), message.getSnapshot().getMeta());

                meta = meta.withConfig(message.getSnapshot().getMeta().getConfig());
                replicatedLog.compactWith(message.getSnapshot(), clusterDiscovery.self());
                for (Any data : message.getSnapshot().getDataList()) {
                    registry.apply(protoService.unpack(data));
                }

                logger.info("response snapshot installed in {} last index {}", meta.getCurrentTerm(), replicatedLog.lastIndex());
                send(message.getLeader(), InstallSnapshotSuccessful.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta.getCurrentTerm()).setLastIndex(replicatedLog.lastIndex()).build());

                return stay(meta);
            }
        }

        private void unstash() throws IOException {
            if (recentlyContactedByLeader.isPresent()) {
                Message poll;
                while ((poll = stashed.poll()) != null) {
                    send(recentlyContactedByLeader.get(), poll);
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

        private FollowerState gotoFollower(RaftMetadata meta) throws IOException {
            return new FollowerState(meta).resetElectionDeadline();
        }

        public State gotoLeader(RaftMetadata meta) throws IOException {
            context.cancelTimer(RaftContext.ELECTION_TIMEOUT);
            return new LeaderState(meta).elected();
        }

        @Override
        public State handle(ClientMessage message) throws IOException {
            stash(message);
            return this;
        }

        @Override
        public State handle(AddServer request) throws IOException {
            send(request.getMember(), AddServerResponse.newBuilder()
                .setStatus(AddServerResponse.Status.NOT_LEADER)
                .build()
            );
            return this;
        }

        @Override
        public State handle(RemoveServer request) throws IOException {
            send(request.getMember(), RemoveServerResponse.newBuilder()
                .setStatus(RemoveServerResponse.Status.NOT_LEADER)
                .build()
            );
            return this;
        }

        public CandidateState resetElectionDeadline() throws IOException {
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

        public State beginElection() throws IOException {
            resetElectionDeadline();
            RaftMetadata meta = meta();
            logger.info("initializing election (among {} nodes) for {}", meta.members().size(), meta.getCurrentTerm());
            RequestVote request = RequestVote.newBuilder()
                .setTerm(meta.getCurrentTerm())
                .setCandidate(clusterDiscovery.self())
                .setLastLogIndex(replicatedLog.lastTerm().orElse(0L))
                .setLastLogTerm(replicatedLog.lastIndex())
                .build();
            for (DiscoveryNode member : meta.membersWithout(clusterDiscovery.self())) {
                if (logger.isInfoEnabled()) {
                    logger.info("send request vote to {}", shortDebugString(member));
                }
                send(member, request);
            }
            meta = meta.incVote().withVoteFor(clusterDiscovery.self());
            if (meta.hasMajority()) {
                if (logger.isInfoEnabled()) {
                    logger.info("received vote by {}, won election with {} of {} votes",
                        shortDebugString(clusterDiscovery.self()), meta.getVotesReceived(), meta.members().size());
                }
                return gotoLeader(meta.forLeader());
            } else {
                return stay(meta);
            }
        }

        @Override
        public State handle(RequestVote message) throws IOException {
            if (message.getTerm() < meta().getCurrentTerm()) {
                if (logger.isInfoEnabled()) {
                    logger.info("rejecting request vote msg by {} in {}, received stale {}",
                        shortDebugString(message.getCandidate()), meta().getCurrentTerm(), message.getTerm());
                }
                send(message.getCandidate(), DeclineCandidate.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta().getCurrentTerm()).build());
                return this;
            }
            if (message.getTerm() > meta().getCurrentTerm()) {
                if (logger.isInfoEnabled()) {
                    logger.info("received newer {}, current term is {}, revert to follower state", message.getTerm(), meta().getCurrentTerm());
                }
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower()).handle(message);
            }
            if (logger.isInfoEnabled()) {
                logger.info("rejecting requestVote msg by {} in {}, already voted for {}",
                    shortDebugString(message.getCandidate()), meta().getCurrentTerm(), meta().getVotedFor().map(TextFormat::shortDebugString));
            }
            send(message.getCandidate(), DeclineCandidate.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta().getCurrentTerm()).build());
            return this;
        }

        @Override
        public State handle(VoteCandidate message) throws IOException {
            RaftMetadata meta = meta();
            if (message.getTerm() < meta.getCurrentTerm()) {
                if (logger.isInfoEnabled()) {
                    logger.info("ignore vote candidate msg by {} in {}, received stale {}",
                        shortDebugString(message.getMember()), meta.getCurrentTerm(), message.getTerm());
                }
                return this;
            }
            if (message.getTerm() > meta.getCurrentTerm()) {
                if (logger.isInfoEnabled()) {
                    logger.info("received newer {}, current term is {}, revert to follower state", message.getTerm(), meta.getCurrentTerm());
                }
                return gotoFollower(meta.withTerm(message.getTerm()).forFollower());
            }

            meta = meta.incVote();
            if (meta.hasMajority()) {
                if (logger.isInfoEnabled()) {
                    logger.info("received vote by {}, won election with {} of {} votes", shortDebugString(message.getMember()), meta.getVotesReceived(), meta.members().size());
                }
                return gotoLeader(meta.forLeader());
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("received vote by {}, have {} of {} votes", shortDebugString(message.getMember()), meta.getVotesReceived(), meta.members().size());
                }
                return stay(meta);
            }
        }

        @Override
        public State handle(DeclineCandidate message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                logger.info("received newer {}, current term is {}, revert to follower state.", message.getTerm(), meta().getCurrentTerm());
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower());
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("candidate is declined by {} in term {}", shortDebugString(message.getMember()), meta().getCurrentTerm());
                }
                return this;
            }
        }

        @Override
        public State handle(AppendEntries message) throws IOException {
            boolean leaderIsAhead = message.getTerm() >= meta().getCurrentTerm();
            if (leaderIsAhead) {
                logger.info("reverting to follower, because got append entries from leader in {}, but am in {}", message.getTerm(), meta().getCurrentTerm());
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower()).handle(message);
            } else {
                return this;
            }
        }

        @Override
        public State handle(InstallSnapshot message) throws IOException {
            boolean leaderIsAhead = message.getTerm() >= meta().getCurrentTerm();
            if (leaderIsAhead) {
                logger.info("reverting to follower, because got install snapshot from leader in {}, but am in {}", message.getTerm(), meta().getCurrentTerm());
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower()).handle(message);
            } else {
                send(message.getLeader(), InstallSnapshotRejected.newBuilder().setMember(clusterDiscovery.self()).setTerm(meta().getCurrentTerm()).build());
                return this;
            }
        }

        public State electionTimeout() throws IOException {
            logger.info("voting timeout, starting a new election (among {})", meta().members().size());
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

        private State gotoFollower(RaftMetadata meta) throws IOException {
            context.cancelTimer(RaftContext.SEND_HEARTBEAT);
            return new FollowerState(meta).resetElectionDeadline();
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
                entry = LogEntry.newBuilder()
                    .setCommand(protoService.pack(meta().getConfig()))
                    .setTerm(meta().getCurrentTerm())
                    .setIndex(replicatedLog.nextIndex())
                    .setClient(clusterDiscovery.self())
                    .build();
            } else {
                entry = LogEntry.newBuilder()
                    .setCommand(protoService.pack(Noop.getDefaultInstance()))
                    .setTerm(meta().getCurrentTerm())
                    .setIndex(replicatedLog.nextIndex())
                    .setClient(clusterDiscovery.self())
                    .build();
            }

            replicatedLog.append(entry);
            matchIndex.put(clusterDiscovery.self(), entry.getIndex());

            sendHeartbeat();
            startHeartbeat();

            ClientMessage clientMessage;
            while ((clientMessage = stashed.poll()) != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("appending command: [{}] from {} to replicated log",
                        shortDebugString(protoService.unpack(clientMessage.getCommand())),
                        shortDebugString(clientMessage.getClient())
                    );
                }
                LogEntry logEntry = LogEntry.newBuilder()
                    .setCommand(clientMessage.getCommand())
                    .setTerm(meta().getCurrentTerm())
                    .setIndex(replicatedLog.nextIndex())
                    .setClient(clientMessage.getClient())
                    .build();
                replicatedLog.append(logEntry);
                matchIndex.put(clusterDiscovery.self(), entry.getIndex());
            }

            return maybeCommitEntry();
        }

        @Override
        public State handle(ClientMessage clientMessage) throws IOException {
            if (logger.isDebugEnabled()) {
                logger.debug("appending command: [{}] from {} to replicated log",
                    shortDebugString(protoService.unpack(clientMessage.getCommand())),
                    shortDebugString(clientMessage.getClient())
                );
            }
            LogEntry entry = LogEntry.newBuilder()
                .setCommand(clientMessage.getCommand())
                .setTerm(meta().getCurrentTerm())
                .setIndex(replicatedLog.nextIndex())
                .setClient(clientMessage.getClient())
                .build();
            replicatedLog.append(entry);
            matchIndex.put(clusterDiscovery.self(), entry.getIndex());
            sendHeartbeat();
            return maybeCommitEntry();
        }

        @Override
        public State handle(AppendEntries message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                if (logger.isInfoEnabled()) {
                    logger.info("leader ({}) got append entries from fresher leader ({}), will step down and the leader will keep being: {}",
                        meta().getCurrentTerm(),
                        message.getTerm(),
                        shortDebugString(message.getMember())
                    );
                }
                return gotoFollower(meta().forFollower()).handle(message);
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("leader ({}) got append entries from rogue leader ({} @ {}), it's not fresher than self, will send entries, to force it to step down.",
                        meta().getCurrentTerm(),
                        shortDebugString(message.getMember()),
                        message.getTerm()
                    );
                }
                sendEntries(message.getMember());
                return this;
            }
        }

        @Override
        public State handle(AppendRejected message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower());
            }
            if (message.getTerm() == meta().getCurrentTerm()) {
                long nextIndexFor = nextIndex.indexFor(message.getMember());
                if (nextIndexFor > message.getLastIndex()) {
                    nextIndex.put(message.getMember(), message.getLastIndex());
                } else if (nextIndexFor > 0) {
                    nextIndex.decrementFor(message.getMember());
                }
                if (logger.isWarnEnabled()) {
                    logger.warn("follower {} rejected write, term {}, decrement index to {}",
                        shortDebugString(message.getMember()),
                        message.getTerm(),
                        nextIndex.indexFor(message.getMember())
                    );
                }
                sendEntries(message.getMember());
                return this;
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("follower {} rejected write: {}, ignore",
                        shortDebugString(message.getMember()),
                        message.getTerm()
                    );
                }
                return this;
            }
        }

        @Override
        public State handle(AppendSuccessful message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower());
            }
            if (message.getTerm() == meta().getCurrentTerm()) {
                if (logger.isInfoEnabled()) {
                    logger.info("received append successful {} in term: {}",
                        shortDebugString(message),
                        meta().getCurrentTerm()
                    );
                }
                assert (message.getLastIndex() <= replicatedLog.lastIndex());
                if (message.getLastIndex() > 0) {
                    nextIndex.put(message.getMember(), message.getLastIndex() + 1);
                }
                matchIndex.putIfGreater(message.getMember(), message.getLastIndex());
                replicationIndex = Immutable.replace(replicationIndex, message.getMember(), 0L);
                maybeSendEntries(message.getMember());
                return maybeCommitEntry();
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("unexpected append successful: {} in term:{}",
                        shortDebugString(message),
                        meta().getCurrentTerm()
                    );
                }
                return this;
            }
        }

        @Override
        public State handle(InstallSnapshot message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                if (logger.isInfoEnabled()) {
                    logger.info("leader ({}) got install snapshot from fresher leader ({}), will step down and the leader will keep being: {}",
                        meta().getCurrentTerm(),
                        message.getTerm(),
                        shortDebugString(message.getLeader())
                    );
                }
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower()).handle(message);
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("rejecting install snapshot {}, current term is {}", message.getTerm(), meta().getCurrentTerm());
                    logger.warn("leader ({}) got install snapshot from rogue leader ({} @ {}), " +
                            "it's not fresher than self, will send entries, to force it to step down.",
                        meta().getCurrentTerm(),
                        shortDebugString(message.getLeader()),
                        message.getTerm()
                    );
                }
                sendEntries(message.getLeader());
                return this;
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
                return maybeCommitEntry();
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("unexpected install snapshot successful: {} in term:{}", shortDebugString(message), meta().getCurrentTerm());
                }
                return this;
            }
        }

        @Override
        public State handle(InstallSnapshotRejected message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                // since there seems to be another leader!
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower());
            } else if (message.getTerm() == meta().getCurrentTerm()) {
                if (logger.isInfoEnabled()) {
                    logger.info("follower {} rejected write: {}, back out the first index in this term and retry",
                        shortDebugString(message.getMember()), message.getTerm());
                }
                if (nextIndex.indexFor(message.getMember()) > 1) {
                    nextIndex.decrementFor(message.getMember());
                }
                sendEntries(message.getMember());
                return this;
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("unexpected install snapshot successful: {} in term:{}",
                        shortDebugString(message), meta().getCurrentTerm());
                }
                return this;
            }
        }

        @Override
        public State handle(AddServer request) throws IOException {
            RaftMetadata meta = meta();
            if (meta.isTransitioning()) {
                if (logger.isWarnEnabled()) {
                    logger.warn("try add server {} in transitioning state", shortDebugString(request.getMember()));
                }
                send(request.getMember(), AddServerResponse.newBuilder()
                    .setStatus(AddServerResponse.Status.TIMEOUT)
                    .setLeader(clusterDiscovery.self())
                    .build()
                );
                return stay(meta);
            } else {
                StableClusterConfiguration stable = StableClusterConfiguration.newBuilder()
                    .addAllMembers(meta.members())
                    .addMembers(request.getMember())
                    .build();
                JointConsensusClusterConfiguration joint = meta.transitionTo(stable);
                ClusterConfiguration jointConfig = ClusterConfiguration.newBuilder().setJoint(joint).build();
                ClusterConfiguration stableConfig = ClusterConfiguration.newBuilder().setStable(stable).build();
                meta = meta.withConfig(jointConfig);
                return stay(meta).handle(ClientMessage.newBuilder()
                    .setClient(request.getMember())
                    .setCommand(protoService.pack(stableConfig)).build());
            }
        }

        @Override
        public State handle(RemoveServer request) throws IOException {
            RaftMetadata meta = meta();
            if (meta.isTransitioning()) {
                if (logger.isWarnEnabled()) {
                    logger.warn("try remove server {} in transitioning state", shortDebugString(request.getMember()));
                }
                send(request.getMember(), RemoveServerResponse.newBuilder()
                    .setStatus(RemoveServerResponse.Status.TIMEOUT)
                    .setLeader(clusterDiscovery.self())
                    .build()
                );
            } else {
                StableClusterConfiguration stable = StableClusterConfiguration.newBuilder()
                    .addAllMembers(meta.membersWithout(request.getMember()))
                    .build();
                JointConsensusClusterConfiguration joint = meta.transitionTo(stable);
                ClusterConfiguration jointConfig = ClusterConfiguration.newBuilder().setJoint(joint).build();
                ClusterConfiguration stableConfig = ClusterConfiguration.newBuilder().setStable(stable).build();
                meta = meta.withConfig(jointConfig);
                return stay(meta).handle(ClientMessage.newBuilder()
                    .setClient(request.getMember())
                    .setCommand(protoService.pack(stableConfig))
                    .build()
                );
            }
            return stay(meta);
        }

        @Override
        public State handle(RequestVote message) throws IOException {
            if (message.getTerm() > meta().getCurrentTerm()) {
                logger.info("received newer {}, current term is {}", message.getTerm(), meta().getCurrentTerm());
                return gotoFollower(meta().withTerm(message.getTerm()).forFollower()).handle(message);
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("rejecting vote for {}, and {}, currentTerm: {}, received stale term number {}",
                        shortDebugString(message.getCandidate()), message.getTerm(),
                        meta().getCurrentTerm(), message.getTerm());
                }
                send(message.getCandidate(), DeclineCandidate.newBuilder()
                    .setMember(clusterDiscovery.self())
                    .setTerm(meta().getCurrentTerm())
                    .build()
                );
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
            if (logger.isDebugEnabled()) {
                logger.debug("send heartbeat: {}", meta().members().stream().map(TextFormat::shortDebugString).collect(Collectors.toList()));
            }
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
                    if (logger.isInfoEnabled()) {
                        logger.info("send install snapshot to {} in term {}", shortDebugString(follower), meta.getCurrentTerm());
                    }
                    send(follower, InstallSnapshot.newBuilder()
                        .setLeader(clusterDiscovery.self())
                        .setTerm(meta.getCurrentTerm())
                        .setSnapshot(snapshot)
                        .build()
                    );
                    return;
                }
            }

            if (lastIndex > replicatedLog.nextIndex()) {
                throw new Error("Unexpected from index " + lastIndex + " > " + replicatedLog.nextIndex());
            } else {
                ImmutableList<LogEntry> entries = replicatedLog.entriesBatchFrom(lastIndex, maxEntries);
                long prevIndex = Math.max(0, lastIndex - 1);
                long prevTerm = replicatedLog.termAt(prevIndex);
                if (logger.isInfoEnabled()) {
                    logger.info("send to {} append entries {} prev {}:{} in {} from index:{}",
                        shortDebugString(follower), entries.size(),
                        prevTerm, prevIndex,
                        meta.getCurrentTerm(), lastIndex);
                }
                AppendEntries append = AppendEntries.newBuilder()
                    .setMember(clusterDiscovery.self())
                    .setTerm(meta.getCurrentTerm())
                    .setPrevLogIndex(prevIndex)
                    .setPrevLogTerm(prevTerm)
                    .addAllEntries(entries)
                    .setLeaderCommit(replicatedLog.committedIndex())
                    .build();
                send(follower, append);
            }
        }

        private State maybeCommitEntry() throws IOException {
            RaftMetadata meta = meta();
            long indexOnMajority;
            while ((indexOnMajority = matchIndex.consensusForIndex(meta)) > replicatedLog.committedIndex()) {
                logger.debug("index of majority: {}", indexOnMajority);
                replicatedLog.slice(replicatedLog.committedIndex(), indexOnMajority);
                ImmutableList<LogEntry> entries = replicatedLog.slice(replicatedLog.committedIndex() + 1, indexOnMajority);
                for (LogEntry entry : entries) {
                    logger.debug("committing log at index: {}", entry.getIndex());
                    replicatedLog.commit(entry.getIndex());
                    if (protoService.is(entry.getCommand(), ClusterConfiguration.getDescriptor())) {
                        ClusterConfiguration config = protoService.unpack(entry.getCommand(), ClusterConfiguration.parser());
                        if (logger.isInfoEnabled()) {
                            logger.info("apply new configuration, old: {}, new: {}", shortDebugString(meta.getConfig()), shortDebugString(config));
                        }
                        meta = meta.withConfig(config);
                        if (!meta.containsOnNewState(clusterDiscovery.self())) {
                            return gotoFollower(meta.forFollower());
                        }
                    } else if (protoService.is(entry.getCommand(), Noop.getDescriptor())) {
                        logger.trace("ignore noop entry");
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("applying command[index={}]: {}, will send result to client: {}", entry.getIndex(), entry.getCommand().getClass(), entry.getClient());
                        }
                        Message result = registry.apply(protoService.unpack(entry.getCommand()));
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
