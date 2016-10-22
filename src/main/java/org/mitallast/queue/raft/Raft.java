package org.mitallast.queue.raft;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.cluster.*;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.domain.*;
import org.mitallast.queue.raft.fsm.FSM;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.raft.log.LogIndexMap;
import org.mitallast.queue.raft.log.ReplicatedLog;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportServer;
import org.mitallast.queue.transport.TransportService;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.mitallast.queue.raft.RaftState.*;

public class Raft extends FSM<RaftState, RaftMetadata> {

    private final TransportService transportService;
    private final TransportServer transportServer;
    private final ClusterDiscovery clusterDiscovery;

    private Optional<DiscoveryNode> recentlyContactedByLeader;

    private ReplicatedLog replicatedLog = new ReplicatedLog();
    private LogIndexMap nextIndex = new LogIndexMap();
    private LogIndexMap matchIndex = new LogIndexMap();

    private final int keepInitUntilFound;
    private long nextIndexDefault = 0;

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
        init();
    }

    private void init() {

        startWith(Init, new RaftMetadata());

        StateFunction initialBehavior = match()
                .event(ChangeConfiguration.class, (conf, meta) -> {
                    logger.info("applying initial raft cluster configuration, consists of [{}] nodes: {}",
                            conf.getNewConf().members().size(),
                            conf.getNewConf().members());
                    resetElectionDeadline();
                    logger.info("finished init of new raft member, becoming follower");
                    return goTo(Follower, apply(new WithNewConfigEvent(meta.getConfig()), meta));
                })
                .event(AppendEntries.class, (request, meta) -> {
                    logger.info("cot append entries from a leader, but am in init state, will ask for it's configuration and join raft cluster");
                    send(request.getMember(), RequestConfiguration.INSTANCE);
                    return goTo(Candidate);
                })
                .event(RaftMemberAdded.class, (added, meta) -> {
                    logger.info("current members: {}, add: {}", meta.members(), added.getMember());
                    ImmutableSet<DiscoveryNode> newMembers = ImmutableSet.<DiscoveryNode>builder()
                            .addAll(meta.members())
                            .add(added.getMember())
                            .build();
                    StableClusterConfiguration initialConfig = new StableClusterConfiguration(0, newMembers);
                    if (added.getKeepInitUntil() <= newMembers.size()) {
                        logger.info("discovered the required min of {} raft cluster members, becoming follower", added.getKeepInitUntil());
                        return goTo(Follower, apply(new WithNewConfigEvent(initialConfig), meta));
                    } else {
                        logger.info("up to {} discovered raft cluster members, still waiting in init until {} discovered.", newMembers.size(), added.getKeepInitUntil());
                        return stay(apply(new WithNewConfigEvent(initialConfig), meta));
                    }
                })
                .event(RaftMemberRemoved.class, (removed, meta) -> {
                    ImmutableSet<DiscoveryNode> newMembers = meta.membersWithout(removed.getMember());
                    StableClusterConfiguration waitingConfig = new StableClusterConfiguration(0, newMembers);
                    logger.debug("removed one member, until now discovered {} raft cluster members, still waiting in init until {} discovered.", newMembers.size(), removed.getKeepInitUntil());
                    return stay(apply(new WithNewConfigEvent(waitingConfig), meta));
                })
                .build();

        StateFunction clusterManagementBehavior = match()
                .event(WhoIsTheLeader.class, (event, meta) -> {
                    switch (currentState()) {
                        case Follower:
                            if (recentlyContactedByLeader.isPresent()) {
                                forward(recentlyContactedByLeader.get(), WhoIsTheLeader.INSTANCE);
                            }
                            break;
                        case Leader:
                            sender().send(new LeaderIs(Optional.of(self()), Optional.empty()));
                            break;
                        default:
                            sender().send(new LeaderIs(Optional.empty(), Optional.empty()));
                            break;
                    }
                    return stay();
                })
                .event(ChangeConfiguration.class, (change, meta) -> {
                    ClusterConfiguration transitioningConfig = meta.getConfig().transitionTo(change.getNewConf());
                    if (!transitioningConfig.transitionToStable().members().equals(meta.getConfig().members())) {
                        logger.info("starting transition to new configuration, old [size: {}]: {}, migrating to [size: {}]: {}",
                                meta.getConfig().members().size(), meta.getConfig().members(),
                                transitioningConfig.transitionToStable().members().size(),
                                transitioningConfig
                        );
                    }
                    receive(new ClientMessage(self(), transitioningConfig));
                    return stay();
                })
                .build();

        StateFunction localClusterBehavior = match()
                .event(RaftMembersDiscoveryTimeout.class, (event, meta) -> {
                    logger.info("discovery timeout");

                    for (HostAndPort hostAndPort : clusterDiscovery.getDiscoveryNodes()) {
                        if (!hostAndPort.equals(self().address())) {
                            try {
                                transportService.connectToNode(hostAndPort);
                                transportService.channel(hostAndPort).send(new MessageTransportFrame(Version.CURRENT, RaftMembersDiscoveryRequest.INSTANCE));
                            } catch (Exception e) {
                                transportService.disconnectFromNode(hostAndPort);
                            }
                        }
                    }
                    setTimer("raft-discovery-timeout", RaftMembersDiscoveryTimeout.INSTANCE, 3, TimeUnit.SECONDS);
                    return stay();
                })
                .event(RaftMembersDiscoveryRequest.class, (event, meta) -> {
                    sender().send(new RaftMembersDiscoveryResponse(self()));
                    return stay();
                })
                .event(RaftMembersDiscoveryResponse.class, (event, meta) -> {
                    logger.info("adding actor {} to raft cluster", event.getMember());
                    receive(new RaftMemberAdded(event.getMember(), keepInitUntilFound));
                    return stay();
                })
                .build();

        StateFunction snapshotBehavior = match()
                .event(InitLogSnapshot.class, (event, meta) -> {
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
                })
                .build();

        StateFunction followerBehavior = match()
                .event(ClientMessage.class, (msg, meta) -> {
                    logger.info("follower got {} from client, respond with last Leader that took write from: {}", msg, recentlyContactedByLeader);
                    sender().send(new LeaderIs(recentlyContactedByLeader, Optional.of(msg)));
                    return stay();
                })
                .event(RequestVote.class, (vote, meta) -> {
                    if (vote.getTerm().greater(meta.getCurrentTerm())) {
                        logger.info("received newer {}, current term is {}", vote.getTerm(), meta.getCurrentTerm());
                        meta = meta.withTerm(vote.getTerm());
                    }
                    if (meta.canVoteIn(vote.getTerm())) {
                        resetElectionDeadline();
                        if (replicatedLog.lastTerm().filter(term -> term.equals(vote.getLastLogTerm())).isPresent()) {
                            logger.warn("rejecting vote for {}, and {}, candidate's lastLogTerm: {} < ours: {}",
                                    vote.getCandidate(),
                                    vote.getTerm(),
                                    vote.getLastLogTerm(),
                                    vote.getLastLogTerm());
                            send(vote.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                            return stay();
                        } else if (replicatedLog.lastTerm().filter(term -> term.equals(vote.getLastLogTerm())).isPresent() &&
                                vote.getLastLogIndex() < replicatedLog.lastIndex()) {
                            logger.warn("rejecting vote for {}, and {}, candidate's lastLogIndex: {} < ours: {}",
                                    vote.getCandidate(),
                                    vote.getTerm(),
                                    vote.getLastLogIndex(),
                                    replicatedLog.lastIndex());
                            send(vote.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                            return stay();
                        } else {
                            logger.info("voting for {} in {}", vote.getCandidate(), vote.getTerm());
                            send(vote.getCandidate(), new VoteCandidate(self(), meta.getCurrentTerm()));
                            return stay(apply(new VoteForEvent(vote.getCandidate()), meta));
                        }
                    } else if (meta.canVoteIn(vote.getTerm())) {
                        resetElectionDeadline();
                        if (replicatedLog.lastTerm().filter(term -> vote.getLastLogTerm().less(term)).isPresent()) {
                            logger.warn("rejecting vote for {}, and {}, candidate's lastLogTerm: {} < ours: {}",
                                    vote.getCandidate(), vote.getTerm(), vote.getLastLogTerm(), replicatedLog.lastTerm());
                            send(vote.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                            return stay();
                        } else if (replicatedLog.lastTerm().filter(term -> term.equals(vote.getLastLogTerm())).isPresent() &&
                                vote.getLastLogIndex() < replicatedLog.lastIndex()) {
                            logger.warn("rejecting vote for {}, and {}, candidate's lastLogIndex: {} < ours: {}",
                                    vote.getCandidate(), vote.getTerm(), vote.getLastLogIndex(), replicatedLog.lastIndex());
                            send(vote.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                            return stay();
                        } else {
                            logger.info("voting for {} in {}", vote.getCandidate(), vote.getTerm());
                            send(vote.getCandidate(), new VoteCandidate(self(), meta.getCurrentTerm()));
                            return stay(apply(new VoteForEvent(vote.getCandidate()), meta));
                        }
                    } else if (meta.getVotedFor().isPresent()) {
                        logger.warn("rejecting vote for {}, and {}, currentTerm: {}, already voted for: {}",
                                vote.getCandidate(),
                                vote.getTerm(),
                                meta.getCurrentTerm(),
                                meta.getVotedFor());
                        send(vote.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                        return stay();
                    } else {
                        logger.warn("rejecting vote for {}, and {}, currentTerm: {}, received stale term number {}",
                                vote.getCandidate(), vote.getTerm(),
                                meta.getCurrentTerm(), vote.getTerm());
                        send(vote.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                        return stay();
                    }
                })
                .event(AppendEntries.class, (request, meta) -> {
                    if (request.getTerm().greater(meta.getCurrentTerm())) {
                        logger.info("received newer {}, current term is {}", request.getTerm(), meta.getCurrentTerm());
                        meta = meta.withTerm(request.getTerm());
                    }
                    if (!replicatedLog.containsMatchingEntry(request.getPrevLogTerm(), request.getPrevLogIndex())) {
                        logger.warn("rejecting write (inconsistent log): {}:{} {} ", request.getPrevLogTerm(), request.getPrevLogIndex(), replicatedLog);
                        send(request.getMember(), new AppendRejected(self(), meta.getCurrentTerm()));
                        return stay();
                    } else {
                        return appendEntries(request, meta);
                    }
                })
                .event(InstallSnapshot.class, (request, meta) -> {
                    if (request.getTerm().greater(meta.getCurrentTerm())) {
                        logger.info("received newer {}, current term is {}", request.getTerm(), meta.getCurrentTerm());
                        meta = meta.withTerm(request.getTerm());
                    }
                    if (request.getTerm().less(meta.getCurrentTerm())) {
                        logger.info("rejecting install snapshot {}, current term is {}", request.getTerm(), meta.getCurrentTerm());
                        send(request.getLeader(), new InstallSnapshotRejected(self(), meta.getCurrentTerm()));
                        return stay();
                    } else {
                        resetElectionDeadline();
                        logger.info("got snapshot from {}, is for: {}", request.getLeader(), request.getSnapshot().getMeta());

                        apply(request);
                        replicatedLog = replicatedLog.compactedWith(request.getSnapshot());

                        logger.info("response snapshot installed in {} last index {}", meta.getCurrentTerm(), replicatedLog.lastIndex());
                        send(request.getLeader(), new InstallSnapshotSuccessful(self(), meta.getCurrentTerm(), replicatedLog.lastIndex()));

                        return stay();
                    }
                })
                .event(ElectionTimeout.class, (event, meta) -> beginElection(meta))
                .event(AskForState.class, (event, meta) -> {
                    sender().send(new IAmInState(Follower));
                    return stay();
                })
                .build();

        StateFunction candidateBehavior = match()
                .event(ClientMessage.class, (msg, meta) -> {
                    logger.info("candidate got {} from client, respond with anarchy - there is no leader", msg);
                    sender().send(new LeaderIs(Optional.empty(), Optional.of(msg)));
                    return stay();
                })
                .event(BeginElection.class, (msg, meta) -> {
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
                })
                .event(RequestVote.class, (request, meta) -> {
                    if (request.getTerm().less(meta.getCurrentTerm())) {
                        logger.info("rejecting request vote msg by {} in {}, received stale {}.", request.getCandidate(), meta.getCurrentTerm(), request.getTerm());
                        send(request.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                        return stay();
                    }
                    if (request.getTerm().greater(meta.getCurrentTerm())) {
                        logger.info("received newer {}, current term is {}, revert to follower state.", request.getTerm(), meta.getCurrentTerm());
                        return stepDown(meta, Optional.of(request.getTerm()));
                    }
                    if (meta.canVoteIn(request.getTerm())) {
                        logger.info("voting for {} in {}", request.getCandidate(), meta.getCurrentTerm());
                        send(request.getCandidate(), new VoteCandidate(self(), meta.getCurrentTerm()));
                        return stay(apply(new VoteForEvent(request.getCandidate()), meta));
                    } else {
                        logger.info("rejecting requestVote msg by {} in {}, already voted for {}", request.getCandidate(), meta.getCurrentTerm(), meta.getVotedFor());
                        send(request.getCandidate(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                        return stay();
                    }
                })
                .event(VoteCandidate.class, (vote, meta) -> {
                    if (vote.getTerm().less(meta.getCurrentTerm())) {
                        logger.info("rejecting vote candidate msg by {} in {}, received stale {}.", vote.getMember(), meta.getCurrentTerm(), vote.getTerm());
                        send(vote.getMember(), new DeclineCandidate(self(), meta.getCurrentTerm()));
                        return stay();
                    }
                    if (vote.getTerm().greater(meta.getCurrentTerm())) {
                        logger.info("received newer {}, current term is {}, revert to follower state.", vote.getTerm(), meta.getCurrentTerm());
                        return stepDown(meta, Optional.of(vote.getTerm()));
                    }

                    int votesReceived = meta.getVotesReceived() + 1;

                    boolean hasWonElection = votesReceived > meta.getConfig().members().size() / 2;
                    if (hasWonElection) {
                        logger.info("received vote by {}, won election with {} of {} votes", vote.getMember(), votesReceived, meta.getConfig().members().size());
                        return goTo(Leader, apply(new GoToLeaderEvent(), meta));
                    } else {
                        logger.info("received vote by {}, have {} of {} votes", vote.getMember(), votesReceived, meta.getConfig().members().size());
                        return stay(apply(new IncrementVoteEvent(), meta));
                    }
                })
                .event(DeclineCandidate.class, (msg, meta) -> {
                    if (msg.getTerm().greater(meta.getCurrentTerm())) {
                        logger.info("received newer {}, current term is {}, revert to follower state.", msg.getTerm(), meta.getCurrentTerm());
                        return stepDown(meta, Optional.of(msg.getTerm()));
                    } else {
                        logger.info("candidate is declined by {} in term {}", msg.getMember(), meta.getCurrentTerm());
                        return stay();
                    }
                })
                .event(AppendEntries.class, (append, meta) -> {
                    boolean leaderIsAhead = append.getTerm().greaterOrEqual(meta.getCurrentTerm());
                    if (leaderIsAhead) {
                        logger.info("reverting to follower, because got append entries from leader in {}, but am in {}", append.getTerm(), meta.getCurrentTerm());
                        forward(self(), append);
                        return stepDown(meta);
                    } else {
                        return stay();
                    }
                })
                .event(ElectionTimeout.class, (event, meta) -> {
                    if (meta.getConfig().members().size() > 1) {
                        logger.info("voting timeout, starting a new election (among {})", meta.getConfig().members().size());
                        receive(BeginElection.INSTANCE);
                        return stay(apply(new StartElectionEvent(), meta));
                    } else {
                        logger.info("voting timeout, unable to start election, don't know enough nodes (members: {})...", meta.getConfig().members().size());
                        return stepDown(meta);
                    }
                })
                .event(AskForState.class, (event, meta) -> {
                    sender().send(new IAmInState(Candidate));
                    return stay();
                })
                .build();

        StateFunction leaderBehavior = match()
                .event(ElectedAsLeader.class, (event, meta) -> {
                    logger.info("became leader for {}", meta.getCurrentTerm());
                    initializeLeaderState();
                    startHeartbeat(meta);
                    return stay();
                })
                .event(SendHeartbeat.class, (event, meta) -> {
                    sendHeartbeat(meta);
                    return stay();
                })
                .event(ElectionMessage.class, (event, meta) -> {
                    logger.debug("got election message");
                    return stay();
                })
                .event(ClientMessage.class, (msg, meta) -> {
                    logger.info("appending command: [{}] from {} to replicated log", msg.getCmd(), msg.getClient());

                    LogEntry entry = new LogEntry(msg.getCmd(), meta.getCurrentTerm(), replicatedLog.nextIndex(), Optional.of(msg.getClient()));

                    logger.debug("adding to log: {}", entry);
                    replicatedLog = replicatedLog.append(entry);
                    matchIndex.put(self(), entry.getIndex());

                    logger.debug("log status = {}", replicatedLog);

                    RaftMetadata updated = maybeUpdateConfiguration(meta, entry.getCommand());
                    if (updated.getConfig().containsOnNewState(self())) {
                        return stay(apply(new KeepStateEvent(), updated));
                    } else {
                        return stepDown(updated);
                    }
                })
                .event(AppendEntries.class, (append, meta) -> {
                    if (append.getTerm().greater(meta.getCurrentTerm())) {
                        logger.info("leader ({}) got append entries from fresher leader ({}), will step down and the leader will keep being: {}", meta.getCurrentTerm(), append.getTerm(), append.getMember());
                        return stepDown(meta);
                    } else {
                        logger.warn("leader ({}) got append entries from rogue leader ({} @ {}), it's not fresher than self, will send entries, to force it to step down.", meta.getCurrentTerm(), append.getMember(), append.getTerm());
                        sendEntries(append.getMember(), meta);
                        return stay();
                    }
                })
                .event(AppendRejected.class, (response, meta) -> {
                    if (response.getTerm().greater(meta.getCurrentTerm())) {
                        return stepDown(meta, Optional.of(response.getTerm()));
                    }
                    if (response.getTerm().equals(meta.getCurrentTerm())) {
                        logger.info("follower {} rejected write: {}, back out the first index in this term and retry", response.getMember(), response.getTerm());
                        if (nextIndex.indexFor(response.getMember()).filter(index -> index > 1).isPresent()) {
                            nextIndex.decrementFor(response.getMember());
                        }
                        return stay();
                    } else {
                        logger.info("follower {} rejected write: {}, ignore", response.getMember(), response.getTerm());
                        return stay();
                    }
                })
                .event(AppendSuccessful.class, (response, meta) -> {
                    if (response.getTerm().equals(meta.getCurrentTerm())) {
                        assert (response.getLastIndex() <= replicatedLog.lastIndex());
                        if (response.getLastIndex() > 0) {
                            nextIndex.put(response.getMember(), response.getLastIndex() + 1);
                        }
                        matchIndex.putIfGreater(response.getMember(), response.getLastIndex());
                        replicatedLog = maybeCommitEntry(meta, matchIndex, replicatedLog);
                        return stay();
                    } else {
                        logger.warn("unexpected append successful: {} in term:{}", response, meta.getCurrentTerm());
                        return stay();
                    }
                })
                .event(InstallSnapshot.class, (request, meta) -> {
                    if (request.getTerm().greater(meta.getCurrentTerm())) {
                        logger.info("leader ({}) got install snapshot from fresher leader ({}), will step down and the leader will keep being: {}",
                                meta.getCurrentTerm(), request.getTerm(), request.getLeader());
                        forward(self(), request);
                        return stepDown(meta, Optional.of(request.getTerm()));
                    } else {
                        logger.info("rejecting install snapshot {}, current term is {}",
                                request.getTerm(), meta.getCurrentTerm());
                        logger.warn("leader ({}) got install snapshot from rogue leader ({} @ {}), " +
                                        "it's not fresher than self, will send entries, to force it to step down.",
                                meta.getCurrentTerm(), request.getLeader(), request.getTerm());
                        sendEntries(request.getLeader(), meta);
                        return stay();
                    }
                })
                .event(InstallSnapshotSuccessful.class, (response, meta) -> {
                    if (response.getTerm().equals(meta.getCurrentTerm())) {
                        assert (response.getLastIndex() <= replicatedLog.lastIndex());
                        if (response.getLastIndex() > 0) {
                            nextIndex.put(response.getMember(), response.getLastIndex() + 1);
                        }
                        matchIndex.putIfGreater(response.getMember(), response.getLastIndex());
                        replicatedLog = maybeCommitEntry(meta, matchIndex, replicatedLog);
                        return stay();
                    } else {
                        logger.warn("unexpected install snapshot successful: {} in term:{}", response, meta.getCurrentTerm());
                        return stay();
                    }
                })
                .event(InstallSnapshotRejected.class, (response, meta) -> {
                    if (response.getTerm().greater(meta.getCurrentTerm())) {
                        // since there seems to be another leader!
                        return stepDown(meta, Optional.of(response.getTerm()));
                    } else if (response.getTerm().equals(meta.getCurrentTerm())) {
                        logger.info("follower {} rejected write: {}, back out the first index in this term and retry", response.getMember(), response.getTerm());
                        if (nextIndex.indexFor(response.getMember()).filter(index -> index > 1).isPresent()) {
                            nextIndex.decrementFor(response.getMember());
                        }
                        return stay();
                    } else {
                        logger.warn("unexpected install snapshot successful: {} in term:{}", response, meta.getCurrentTerm());
                        return stay();
                    }
                })
                .event(RequestConfiguration.class, (request, meta) -> {
                    sender().send(new ChangeConfiguration(meta.getConfig()));
                    return stay();
                })
                .event(AskForState.class, (request, meta) -> {
                    sender().send(new IAmInState(Leader));
                    return stay();
                })
                .build();

        when(Init, initialBehavior.orElse(localClusterBehavior));
        when(Follower, followerBehavior.orElse(snapshotBehavior).orElse(clusterManagementBehavior).orElse(localClusterBehavior));
        when(Candidate, candidateBehavior.orElse(snapshotBehavior).orElse(clusterManagementBehavior).orElse(localClusterBehavior));
        when(Leader, leaderBehavior.orElse(snapshotBehavior).orElse(clusterManagementBehavior).orElse(localClusterBehavior));
    }

    @Override
    public void onStart() {
        receive(RaftMembersDiscoveryTimeout.INSTANCE);
        receive(new RaftMemberAdded(self(), keepInitUntilFound));
    }

    @Override
    public void onTransition(RaftState prevState, RaftState newState) {
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

    @Override
    public void onTermination() {
        stopHeartbeat();
    }

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
        setTimer("raft-heartbeat", SendHeartbeat.INSTANCE, 1000, TimeUnit.MILLISECONDS, true);
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

    public RaftMetadata apply(DomainEvent domainEvent, RaftMetadata meta) {
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

    public Streamable apply(Streamable message) {
        logger.info("apply: {}", message);
        return null;
    }

    public void cancelElectionDeadline() {
        cancelTimer("raft-election-timeout");
    }

    public void resetElectionDeadline() {
        cancelElectionDeadline();
        long timeout = new Random().nextInt(1000) + 2000;
        setTimer("raft-election-timeout", ElectionTimeout.INSTANCE, timeout, TimeUnit.MILLISECONDS);
    }

    public State beginElection(RaftMetadata meta) {
        resetElectionDeadline();
        if (meta.getConfig().members().isEmpty()) {
            return goTo(Follower, apply(new KeepStateEvent(), meta));
        } else {
            return goTo(Candidate, apply(new StartElectionEvent(), meta));
        }
    }

    public State stepDown(RaftMetadata meta) {
        return stepDown(meta, Optional.empty());
    }

    public State stepDown(RaftMetadata meta, Optional<Term> term) {
        return goTo(Follower, apply(new GoToFollowerEvent(term), meta));
    }

    public State acceptHeartbeat() {
        resetElectionDeadline();
        return stay();
    }

    public DiscoveryNode self() {
        return transportServer.localNode();
    }

    public void forward(DiscoveryNode node, Streamable message) {
        transportService.channel(node.address()).send(new MessageTransportFrame(Version.CURRENT, message));
    }

    public void send(DiscoveryNode node, Streamable message) {
        if(node.equals(self())) {
            receive(message);
        }else{
            transportService.channel(node.address()).send(new MessageTransportFrame(Version.CURRENT, message));
        }
    }

    @SuppressWarnings("UnusedParameters")
    public CompletableFuture<Optional<RaftSnapshot>> prepareSnapshot(RaftSnapshotMetadata snapshotMeta) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    public State appendEntries(AppendEntries msg, RaftMetadata meta) {
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
            logger.debug("committing entry {} on follower, leader is committed until [{}]", entry, msg.getLeaderCommit());
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
            logger.debug("executing: replicatedLog = replicatedLog.append({}, {})", entries, atIndex - 1);
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
}
