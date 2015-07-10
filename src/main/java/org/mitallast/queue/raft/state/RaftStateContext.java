package org.mitallast.queue.raft.state;

import com.google.inject.Inject;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.unit.TimeValue;
import org.mitallast.queue.raft.NoLeaderException;
import org.mitallast.queue.raft.Protocol;
import org.mitallast.queue.raft.Query;
import org.mitallast.queue.raft.action.join.JoinRequest;
import org.mitallast.queue.raft.action.join.JoinResponse;
import org.mitallast.queue.raft.action.leave.LeaveRequest;
import org.mitallast.queue.raft.action.leave.LeaveResponse;
import org.mitallast.queue.raft.cluster.Member;
import org.mitallast.queue.raft.cluster.TransportCluster;
import org.mitallast.queue.raft.log.Log;
import org.mitallast.queue.raft.log.compaction.Compactor;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RaftStateContext extends RaftStateClient implements Protocol {
    private final RaftState stateMachine;
    private final Log log;
    private final Compactor compactor;
    private final TransportCluster transportCluster;
    private final ClusterState members;
    private final ExecutionContext executionContext;
    private final long electionTimeout;
    private final long heartbeatInterval;

    private volatile AbstractState state;
    private volatile ScheduledFuture<?> joinTimer;

    private volatile DiscoveryNode lastVotedFor;
    private volatile long commitIndex;
    private volatile long globalIndex;

    @Inject
    public RaftStateContext(Settings settings, RaftState raftState, Log log, Compactor compactor, TransportCluster transportCluster, ClusterState clusterState, ExecutionContext executionContext) throws ExecutionException, InterruptedException {
        super(settings, transportCluster, executionContext);
        this.log = log;
        this.stateMachine = raftState;
        this.compactor = compactor;
        this.transportCluster = transportCluster;
        this.members = clusterState;
        this.executionContext = executionContext;
        this.electionTimeout = componentSettings.getAsTime("election_timeout", TimeValue.timeValueSeconds(1)).millis();
        this.heartbeatInterval = componentSettings.getAsTime("heartbeat_interval", TimeValue.timeValueSeconds(1)).millis();
        executionContext.submit(() -> {
            transition(StartState.class);
            for (Member member : transportCluster.members()) {
                members.addMember(new MemberState(member.node(), member.type()));
            }
        }).get();
    }


    public long getElectionTimeout() {
        executionContext.checkThread();
        return electionTimeout;
    }

    public long getHeartbeatInterval() {
        executionContext.checkThread();
        return heartbeatInterval;
    }

    ClusterState getMembers() {
        executionContext.checkThread();
        return members;
    }

    @Override
    RaftStateContext setLeader(DiscoveryNode leader) {
        executionContext.checkThread();
        if (this.leader == null) {
            if (leader != null) {
                this.leader = leader;
                this.lastVotedFor = null;
                logger.info("found leader {}", leader);
            }
        } else if (leader != null) {
            if (this.leader != leader) {
                this.leader = leader;
                this.lastVotedFor = null;
                logger.info("found leader {}", leader);
            }
        } else {
            this.leader = null;
        }
        return this;
    }

    RaftStateContext setTerm(long term) {
        executionContext.checkThread();
        if (term > this.term) {
            this.term = term;
            this.leader = null;
            this.lastVotedFor = null;
            logger.info("incremented term {}", term);
        }
        return this;
    }

    public DiscoveryNode getLastVotedFor() {
        executionContext.checkThread();
        return lastVotedFor;
    }

    RaftStateContext setLastVotedFor(DiscoveryNode candidate) {
        executionContext.checkThread();
        // If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
        if (lastVotedFor != null && candidate != null) {
            throw new IllegalStateException("Already voted for another candidate");
        }
        if (leader != null && candidate != null) {
            throw new IllegalStateException("Cannot cast vote - leader already exists");
        }
        this.lastVotedFor = candidate;
        if (candidate != null) {
            logger.info("voted for {}", candidate);
        } else {
            logger.info("reset last voted for");
        }
        return this;
    }

    public long getCommitIndex() {
        executionContext.checkThread();
        return commitIndex;
    }

    RaftStateContext setCommitIndex(long commitIndex) {
        executionContext.checkThread();
        if (commitIndex < 0)
            throw new IllegalArgumentException("commit index must be positive");
        if (commitIndex < this.commitIndex)
            throw new IllegalArgumentException("cannot decrease commit index");
        this.commitIndex = commitIndex;
        compactor.setCommitIndex(commitIndex);
        return this;
    }

    public long getGlobalIndex() {
        executionContext.checkThread();
        return globalIndex;
    }

    RaftStateContext setGlobalIndex(long globalIndex) {
        executionContext.checkThread();
        if (globalIndex < 0)
            throw new IllegalArgumentException("global index must be positive");
        this.globalIndex = Math.max(this.globalIndex, globalIndex);
        compactor.setCompactIndex(globalIndex);
        return this;
    }

    public AbstractState raftState() {
        return state;
    }

    public RaftStateType getState() {
        return state.type();
    }

    RaftState getStateMachine() {
        executionContext.checkThread();
        return stateMachine;
    }

    public Log getLog() {
        return log;
    }

    @Override
    protected Member selectMember(Query<?> query) {
        executionContext.checkThread();
        if (!query.consistency().isLeaderRequired()) {
            return transportCluster.member();
        }
        return super.selectMember(query);
    }

    @Override
    protected CompletableFuture<Void> register(List<Member> members) {
        executionContext.checkThread();
        return register(members, Futures.future())
            .thenAcceptAsync(response -> setSession(response.session()), executionContext.executor());
    }

    @Override
    protected CompletableFuture<Void> keepAlive(List<Member> members) {
        executionContext.checkThread();
        return keepAlive(members, Futures.future())
            .thenAcceptAsync(response -> setVersion(response.version()), executionContext.executor());
    }

    synchronized CompletableFuture<RaftStateType> transition(Class<? extends AbstractState> state) {
        executionContext.checkThread();
        if (this.state != null && state == this.state.getClass()) {
            return Futures.complete(this.state.type());
        }

        logger.info("transitioning to {}", state.getSimpleName());

        // Force state transitions to occur synchronously in order to prevent race conditions.
        try {
            if (this.state != null) {
                this.state.close();
            }
            Constructor<? extends AbstractState> constructor = state.getConstructor(Settings.class, RaftStateContext.class, ExecutionContext.class, TransportCluster.class);
            if (constructor == null) {
                logger.error("Error find constructor for {}", state);
                throw new IOException("Error find constructor for " + state);
            }
            this.state = constructor.newInstance(this.settings, this, executionContext, transportCluster);
            this.state.open();
        } catch (Exception e) {
            logger.error("error transitioning", e);
            throw new IllegalStateException("failed to initialize Raft state", e);
        }
        return Futures.complete(null);
    }

    private CompletableFuture<Void> join() {
        executionContext.checkThread();
        CompletableFuture<Void> future = Futures.future();
        executionContext.execute(() -> join(100, future));
        return future;
    }

    private CompletableFuture<Void> join(long interval, CompletableFuture<Void> future) {
        executionContext.checkThread();
        join(new ArrayList<>(transportCluster.members()), Futures.future()).whenComplete((result, error) -> {
            executionContext.checkThread();
            if (error == null) {
                future.complete(null);
            } else {
                long nextInterval = Math.min(interval * 2, 5000);
                joinTimer = executionContext.schedule(() -> join(nextInterval, future), nextInterval, TimeUnit.MILLISECONDS);
            }
        });
        return future;
    }

    private CompletableFuture<Void> join(List<Member> members, CompletableFuture<Void> future) {
        executionContext.checkThread();
        if (members.isEmpty()) {
            future.completeExceptionally(new NoLeaderException("no leader found"));
            return future;
        }
        return join(selectMember(members), members, future);
    }

    private CompletableFuture<Void> join(Member member, List<Member> members, CompletableFuture<Void> future) {
        executionContext.checkThread();
        logger.info("joining cluster via {}", member.node());
        JoinRequest request = JoinRequest.builder()
            .setMember(transportCluster.member().node())
            .build();
        member.<JoinRequest, JoinResponse>send(request).whenCompleteAsync((response, error) -> {
            if (error == null) {
                setLeader(response.leader());
                setTerm(response.term());
                future.complete(null);
                logger.info("joined cluster");
            } else {
                logger.info("cluster join failed, retrying");
                setLeader(null);
                join(members, future);
            }
        }, executionContext.executor());
        return future;
    }

    private CompletableFuture<Void> leave() {
        executionContext.checkThread();
        return leave(transportCluster.members().stream()
            .filter(m -> m.type() == Member.Type.ACTIVE)
            .collect(Collectors.toList()), Futures.future());
    }

    private CompletableFuture<Void> leave(List<Member> members, CompletableFuture<Void> future) {
        executionContext.checkThread();
        if (members.isEmpty()) {
            future.completeExceptionally(new NoLeaderException("no leader found"));
            return future;
        }
        return leave(selectMember(members), members, future);
    }

    private CompletableFuture<Void> leave(Member member, List<Member> members, CompletableFuture<Void> future) {
        executionContext.checkThread();
        logger.info("leaving cluster via {}", member.node());
        LeaveRequest request = LeaveRequest.builder()
            .setMember(transportCluster.member().node())
            .build();
        member.<LeaveRequest, LeaveResponse>send(request).whenCompleteAsync((response, error) -> {
            if (error == null) {
                future.complete(null);
                logger.info("left cluster");
            } else {
                logger.info("cluster leave failed, retrying");
                setLeader(null);
                leave(members, future);
            }
        }, executionContext.executor());
        return future;
    }

    private void cancelJoinTimer() {
        executionContext.checkThread();
        if (joinTimer != null) {
            logger.info("cancelling join timer");
            joinTimer.cancel(false);
        }
    }

    @Override
    public void delete() {
        executionContext.checkThread();
        if (log != null) {
            try {
                log.delete();
            } catch (IOException e) {
                logger.error("error delete log", e);
            }
        }
    }

    @Override
    protected void doStop() throws IOException {
        try {
            executionContext.submit(() -> {
                cancelJoinTimer();
                transition(StartState.class);
                leave();
            }).get();
            super.doStop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void doStart() throws IOException {
        super.doStart();
        try {
            executionContext.submit(() -> {
                if (transportCluster.member().type() == Member.Type.PASSIVE) {
                    transition(PassiveState.class);
                    join();
                } else {
                    transition(FollowerState.class);
                }
            }).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }
}
