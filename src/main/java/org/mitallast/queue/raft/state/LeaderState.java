package org.mitallast.queue.raft.state;

import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.*;
import org.mitallast.queue.raft.action.ResponseStatus;
import org.mitallast.queue.raft.action.append.AppendRequest;
import org.mitallast.queue.raft.action.append.AppendResponse;
import org.mitallast.queue.raft.action.command.CommandRequest;
import org.mitallast.queue.raft.action.command.CommandResponse;
import org.mitallast.queue.raft.action.join.JoinRequest;
import org.mitallast.queue.raft.action.join.JoinResponse;
import org.mitallast.queue.raft.action.keepalive.KeepAliveRequest;
import org.mitallast.queue.raft.action.keepalive.KeepAliveResponse;
import org.mitallast.queue.raft.action.leave.LeaveRequest;
import org.mitallast.queue.raft.action.leave.LeaveResponse;
import org.mitallast.queue.raft.action.query.QueryRequest;
import org.mitallast.queue.raft.action.query.QueryResponse;
import org.mitallast.queue.raft.action.register.RegisterRequest;
import org.mitallast.queue.raft.action.register.RegisterResponse;
import org.mitallast.queue.raft.action.vote.VoteRequest;
import org.mitallast.queue.raft.action.vote.VoteResponse;
import org.mitallast.queue.raft.cluster.Cluster;
import org.mitallast.queue.raft.cluster.Member;
import org.mitallast.queue.raft.log.entry.*;
import org.mitallast.queue.raft.util.ExecutionContext;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class LeaderState extends ActiveState {
    private final Replicator replicator = new Replicator();
    private volatile ScheduledFuture<?> currentTimer;

    public LeaderState(Settings settings, RaftStateContext context, ExecutionContext executionContext, Cluster cluster) {
        super(settings, context, executionContext, cluster);
    }

    @Override
    public Raft.State type() {
        return Raft.State.LEADER;
    }

    public synchronized void open() {
        executionContext.checkThread();
        // Schedule the initial entries commit to occur after the state is opened. Attempting any communication
        // within the open() method will result in a deadlock since RaftProtocol calls this method synchronously.
        // What is critical about this logic is that the heartbeat timer not be started until a NOOP entry has been committed.
        try {
            commitEntries().whenComplete((result, error) -> {
                if (error == null) {
                    startHeartbeatTimer();
                }
            });
        } catch (IOException e) {
            logger.error("error commit", e);
        }
        takeLeadership();
    }

    /**
     * Sets the current node as the cluster leader.
     */
    private void takeLeadership() {
        executionContext.checkThread();
        context.setLeader(cluster.member().node());
    }

    /**
     * Commits a no-op entry to the log, ensuring any entries from a previous term are committed.
     */
    private CompletableFuture<Void> commitEntries() throws IOException {
        executionContext.checkThread();
        final long term = context.getTerm();
        final long index;
        NoOpEntry entry = NoOpEntry.builder()
            .setTerm(term)
            .setIndex(context.getLog().nextIndex())
            .build();
        index = context.getLog().appendEntry(entry);

        CompletableFuture<Void> future = Futures.future();
        replicator.commit(index).whenComplete((resultIndex, error) -> {
            if (error == null) {
                try {
                    applyEntries(resultIndex);
                    future.complete(null);
                } catch (IOException e) {
                    future.completeExceptionally(e);
                }
            } else {
                transition(Raft.State.FOLLOWER);
            }
        });
        return future;
    }

    /**
     * Applies all unapplied entries to the log.
     */
    private void applyEntries(long index) throws IOException {
        executionContext.checkThread();
        if (!context.getLog().isEmpty()) {
            int count = 0;
            for (long lastApplied = Math.max(context.getStateMachine().getLastApplied(), context.getLog().firstIndex()); lastApplied <= index; lastApplied++) {
                LogEntry entry = context.getLog().getEntry(lastApplied);
                if (entry != null) {
                    context.getStateMachine().apply(entry).whenComplete((result, error) -> {
                        executionContext.checkThread();
                        if (error != null) {
                            logger.warn("application error occurred: {}", error);
                        }
                    });
                }
                count++;
            }
            logger.debug("applied {} entries to log", count);
        }
    }

    /**
     * Starts heartbeating all cluster members.
     */
    private void startHeartbeatTimer() {
        executionContext.checkThread();
        // Set a timer that will be used to periodically synchronize with other nodes
        // in the cluster. This timer acts as a heartbeat to ensure this node remains
        // the leader.
        logger.debug("starting heartbeat timer");
        currentTimer = executionContext.scheduleAtFixedRate(this::heartbeatMembers, 0, context.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
    }

    private void stopHeartbeatTimer() {
        executionContext.checkThread();
        if (currentTimer != null) {
            logger.debug("cancelling heartbeat timer");
            currentTimer.cancel(false);
        }
    }

    /**
     * Sends a heartbeat to all members of the cluster.
     */
    private void heartbeatMembers() {
        executionContext.checkThread();
        replicator.commit();
    }

    @Override
    public CompletableFuture<VoteResponse> vote(final VoteRequest request) {
        executionContext.checkThread();
        if (request.term() > context.getTerm()) {
            logger.info("received greater term");
            transition(Raft.State.FOLLOWER);
            return super.vote(request);
        } else {
            return Futures.complete(VoteResponse.builder()
                .setStatus(ResponseStatus.OK)
                .setTerm(context.getTerm())
                .setVoted(false)
                .build());
        }
    }

    @Override
    public CompletableFuture<AppendResponse> append(final AppendRequest request) {
        executionContext.checkThread();
        if (request.term() > context.getTerm()) {
            return super.append(request);
        } else if (request.term() < context.getTerm()) {
            return Futures.complete(AppendResponse.builder()
                .setStatus(ResponseStatus.OK)
                .setTerm(context.getTerm())
                .setSucceeded(false)
                .setLogIndex(context.getLog().lastIndex())
                .build());
        } else {
            transition(Raft.State.FOLLOWER);
            return super.append(request);
        }
    }

    @Override
    public CompletableFuture<CommandResponse> command(final CommandRequest request) {
        executionContext.checkThread();
        Command command = request.command();
        final long term = context.getTerm();
        final long timestamp = System.currentTimeMillis();
        final long index;

        try {
            CommandEntry entry = CommandEntry.builder()
                .setTerm(term)
                .setIndex(context.getLog().nextIndex())
                .setSession(request.session())
                .setRequest(request.request())
                .setResponse(request.response())
                .setTimestamp(timestamp)
                .setCommand(command)
                .build();
            index = context.getLog().appendEntry(entry);
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }
        logger.debug("appended entry to log at index {}", index);


        CompletableFuture<CommandResponse> future = Futures.future();
        replicator.commit(index).whenComplete((commitIndex, commitError) -> {

            if (commitError == null) {
                try {
                    CommandEntry entry = context.getLog().getEntry(index);
                    if (entry != null) {
                        context.getStateMachine().apply(entry).whenComplete((result, error) -> {
                            executionContext.checkThread();
                            if (error == null) {
                                future.complete(CommandResponse.builder()
                                    .setStatus(ResponseStatus.OK)
                                    .setResult(result)
                                    .build());
                            } else {
                                logger.error("command error", error);
                                future.complete(CommandResponse.builder()
                                    .setStatus(ResponseStatus.ERROR)
                                    .setError(RaftError.INTERNAL_ERROR)
                                    .build());
                            }
                        });
                    } else {
                        future.complete(CommandResponse.builder()
                            .setStatus(ResponseStatus.OK)
                            .setResult(null)
                            .build());
                    }
                } catch (IOException e) {
                    logger.error("error apply entry", e);
                    future.complete(CommandResponse.builder()
                        .setStatus(ResponseStatus.ERROR)
                        .setError(RaftError.INTERNAL_ERROR)
                        .build());
                }
            } else {
                logger.error("error apply entry", commitError);
                future.complete(CommandResponse.builder()
                    .setStatus(ResponseStatus.ERROR)
                    .setError(RaftError.PROTOCOL_ERROR)
                    .build());
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<QueryResponse> query(final QueryRequest request) {
        executionContext.checkThread();
        Query query = request.query();

        final long timestamp = System.currentTimeMillis();
        final long index = context.getCommitIndex();

        QueryEntry entry = QueryEntry.builder()
            .setIndex(index)
            .setTerm(context.getTerm())
            .setSession(request.session())
            .setVersion(request.version())
            .setTimestamp(timestamp)
            .setQuery(query)
            .build();

        ConsistencyLevel consistency = query.consistency();
        if (consistency == null)
            return submitQueryLinearizableStrict(entry);

        switch (consistency) {
            case SERIALIZABLE:
                return submitQuerySerializable(entry);
            case LINEARIZABLE_LEASE:
                return submitQueryLinearizableLease(entry);
            case LINEARIZABLE_STRICT:
                return submitQueryLinearizableStrict(entry);
            default:
                throw new IllegalStateException("unknown consistency level");
        }
    }

    /**
     * Submits a query with serializable consistency.
     */
    private CompletableFuture<QueryResponse> submitQuerySerializable(QueryEntry entry) {
        executionContext.checkThread();
        return applyQuery(entry, Futures.future());
    }

    /**
     * Submits a query with lease based linearizable consistency.
     */
    private CompletableFuture<QueryResponse> submitQueryLinearizableLease(QueryEntry entry) {
        executionContext.checkThread();
        long commitTime = replicator.commitTime();
        if (System.currentTimeMillis() - commitTime < context.getElectionTimeout()) {
            return submitQuerySerializable(entry);
        } else {
            return submitQueryLinearizableStrict(entry);
        }
    }

    /**
     * Submits a query with strict linearizable consistency.
     */
    private CompletableFuture<QueryResponse> submitQueryLinearizableStrict(QueryEntry entry) {
        executionContext.checkThread();
        CompletableFuture<QueryResponse> future = Futures.future();
        replicator.commit().whenComplete((commitIndex, commitError) -> {
            if (commitError == null) {
                applyQuery(entry, future);
            } else {
                logger.error("commitError error", commitError);
                future.complete(QueryResponse.builder()
                    .setStatus(ResponseStatus.ERROR)
                    .setError(RaftError.COMMAND_ERROR)
                    .build());
            }
        });
        return future;
    }

    /**
     * Applies a query to the state machine.
     */
    private CompletableFuture<QueryResponse> applyQuery(QueryEntry entry, CompletableFuture<QueryResponse> future) {
        executionContext.checkThread();
        long version = context.getStateMachine().getLastApplied();
        context.getStateMachine().apply(entry).whenComplete((result, error) -> {
            if (error == null) {
                future.complete(QueryResponse.builder()
                    .setStatus(ResponseStatus.OK)
                    .setVersion(version)
                    .setResult(result)
                    .build());
            } else if (error instanceof ApplicationException) {
                logger.error("application error", error);
                future.complete(QueryResponse.builder()
                    .setStatus(ResponseStatus.ERROR)
                    .setError(RaftError.APPLICATION_ERROR)
                    .build());
            } else {
                logger.error("internal error", error);
                future.complete(QueryResponse.builder()
                    .setStatus(ResponseStatus.ERROR)
                    .setError(RaftError.INTERNAL_ERROR)
                    .build());
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<RegisterResponse> register(RegisterRequest request) {
        executionContext.checkThread();
        final long timestamp = System.currentTimeMillis();
        final long index;

        try {
            RegisterEntry entry = RegisterEntry.builder()
                .setTerm(context.getTerm())
                .setIndex(context.getLog().nextIndex())
                .setTimestamp(timestamp)
                .build();
            index = context.getLog().appendEntry(entry);
            logger.debug("appended register entry {}", entry);
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }

        CompletableFuture<RegisterResponse> future = Futures.future();
        replicator.commit(index).whenComplete((commitIndex, commitError) -> {
            if (commitError == null) {
                try {
                    RegisterEntry entry = context.getLog().getEntry(index);
                    context.getStateMachine().apply(entry).whenComplete((sessionId, sessionError) -> {
                        if (sessionError == null) {
                            future.complete(RegisterResponse.builder()
                                .setStatus(ResponseStatus.OK)
                                .setLeader(context.getLeader())
                                .setTerm(context.getTerm())
                                .setSession(sessionId)
                                .setMembers(cluster.members().stream().map(Member::node).collect(Collectors.toList()))
                                .build());
                        } else if (sessionError instanceof ApplicationException) {
                            logger.error("application error", sessionError);
                            future.complete(RegisterResponse.builder()
                                .setStatus(ResponseStatus.ERROR)
                                .setError(RaftError.APPLICATION_ERROR)
                                .build());
                        } else {
                            logger.error("session error", sessionError);
                            future.complete(RegisterResponse.builder()
                                .setStatus(ResponseStatus.ERROR)
                                .setError(RaftError.INTERNAL_ERROR)
                                .build());
                        }
                    });
                } catch (IOException e) {
                    logger.error("io error", e);
                    future.complete(RegisterResponse.builder()
                        .setStatus(ResponseStatus.ERROR)
                        .setError(RaftError.INTERNAL_ERROR)
                        .build());
                }
            } else {
                logger.error("commit error", commitError);
                future.complete(RegisterResponse.builder()
                    .setStatus(ResponseStatus.ERROR)
                    .setError(RaftError.PROTOCOL_ERROR)
                    .build());
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
        executionContext.checkThread();
        final long timestamp = System.currentTimeMillis();
        final long index;

        try {
            KeepAliveEntry entry = KeepAliveEntry.builder()
                .setTerm(context.getTerm())
                .setIndex(context.getLog().nextIndex())
                .setSession(request.session())
                .setTimestamp(timestamp)
                .build();

            index = context.getLog().appendEntry(entry);
            logger.debug("appended session entry to log at index {}", index);
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }

        CompletableFuture<KeepAliveResponse> future = Futures.future();
        replicator.commit(index).whenComplete((commitIndex, commitError) -> {
            if (commitError == null) {
                try {
                    KeepAliveEntry entry = context.getLog().getEntry(index);
                    long version = context.getStateMachine().getLastApplied();
                    context.getStateMachine().apply(entry).whenComplete((sessionResult, sessionError) -> {
                        executionContext.checkThread();
                        if (sessionError == null) {
                            future.complete(KeepAliveResponse.builder()
                                .setStatus(ResponseStatus.OK)
                                .setLeader(context.getLeader())
                                .setTerm(context.getTerm())
                                .setVersion(version)
                                .setMembers(cluster.members().stream().map(Member::node).collect(Collectors.toList()))
                                .build());
                        } else if (sessionError instanceof ApplicationException) {
                            logger.error("application error", sessionError);
                            future.complete(KeepAliveResponse.builder()
                                .setStatus(ResponseStatus.ERROR)
                                .setError(RaftError.APPLICATION_ERROR)
                                .build());
                        } else {
                            logger.error("session error", sessionError);
                            future.complete(KeepAliveResponse.builder()
                                .setStatus(ResponseStatus.ERROR)
                                .setError(RaftError.INTERNAL_ERROR)
                                .build());
                        }
                    });
                } catch (IOException e) {
                    logger.error("io error", e);
                    future.complete(KeepAliveResponse.builder()
                        .setStatus(ResponseStatus.ERROR)
                        .setError(RaftError.INTERNAL_ERROR)
                        .build());
                }
            } else {
                logger.error("commit error", commitError);
                future.complete(KeepAliveResponse.builder()
                    .setStatus(ResponseStatus.ERROR)
                    .setError(RaftError.PROTOCOL_ERROR)
                    .build());
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<JoinResponse> join(JoinRequest request) {
        executionContext.checkThread();
        final long index;

        try {
            JoinEntry entry = JoinEntry.builder()
                .setTerm(context.getTerm())
                .setIndex(context.getLog().nextIndex())
                .setMember(request.member())
                .build();
            index = context.getLog().appendEntry(entry);
            logger.debug("appended {}", entry);
        } catch (Throwable e) {
            return Futures.completeExceptionally(e);
        }

        CompletableFuture<JoinResponse> future = Futures.future();
        replicator.commit(index).whenComplete((commitIndex, commitError) -> {
            if (commitError == null) {
                try {
                    JoinEntry entry = context.getLog().getEntry(index);
                    context.getStateMachine().apply(entry).whenComplete((sessionId, sessionError) -> {
                        executionContext.checkThread();
                        if (sessionError == null) {
                            future.complete(JoinResponse.builder()
                                .setStatus(ResponseStatus.OK)
                                .setLeader(context.getLeader())
                                .setTerm(context.getTerm())
                                .build());
                        } else {
                            logger.error("session error", sessionError);
                            future.complete(JoinResponse.builder()
                                .setStatus(ResponseStatus.ERROR)
                                .setError(RaftError.INTERNAL_ERROR)
                                .build());
                        }
                    });
                } catch (IOException e) {
                    logger.error("io error", e);
                    future.complete(JoinResponse.builder()
                        .setStatus(ResponseStatus.ERROR)
                        .setError(RaftError.PROTOCOL_ERROR)
                        .build());
                }
            } else {
                logger.error("commit error", commitError);
                future.complete(JoinResponse.builder()
                    .setStatus(ResponseStatus.ERROR)
                    .setError(RaftError.PROTOCOL_ERROR)
                    .build());
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
        executionContext.checkThread();
        final long index;

        try {
            LeaveEntry entry = LeaveEntry.builder()
                .setTerm(context.getTerm())
                .setIndex(context.getLog().nextIndex())
                .setMember(request.member())
                .build();

            index = context.getLog().appendEntry(entry);
            logger.debug("appended {}", entry);
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }

        CompletableFuture<LeaveResponse> future = Futures.future();
        replicator.commit(index).whenComplete((commitIndex, commitError) -> {
            if (commitError == null) {
                try {
                    LeaveEntry entry = context.getLog().getEntry(index);
                    context.getStateMachine().apply(entry).whenComplete((sessionId, sessionError) -> {
                        executionContext.checkThread();
                        if (sessionError == null) {
                            future.complete(LeaveResponse.builder()
                                .setStatus(ResponseStatus.OK)
                                .build());
                        } else {
                            logger.error("session error", sessionError);
                            future.complete(LeaveResponse.builder()
                                .setStatus(ResponseStatus.ERROR)
                                .setError(RaftError.INTERNAL_ERROR)
                                .build());
                        }
                    });
                } catch (IOException e) {
                    logger.error("io error", e);
                    future.complete(LeaveResponse.builder()
                        .setStatus(ResponseStatus.ERROR)
                        .setError(RaftError.INTERNAL_ERROR)
                        .build());
                }
            } else {
                logger.error("commit error", commitError);
                future.complete(LeaveResponse.builder()
                    .setStatus(ResponseStatus.ERROR)
                    .setError(RaftError.PROTOCOL_ERROR)
                    .build());
            }
        });
        return future;
    }

    public synchronized void close() {
        executionContext.checkThread();
        logger.debug("close leader state");
        stopHeartbeatTimer();
    }

    private class Replicator {
        private final List<Replica> replicas = new ArrayList<>();
        private final List<Long> commitTimes = new ArrayList<>();
        private final TreeMap<Long, CompletableFuture<Long>> commitFutures = new TreeMap<>();
        private volatile long commitTime;
        private volatile CompletableFuture<Long> commitFuture;
        private volatile CompletableFuture<Long> nextCommitFuture;
        private volatile int quorum;
        private volatile int quorumIndex;

        private Replicator() {
            Set<Member> members = cluster.members().stream()
                // not local and only active
                .filter(m -> !m.node().equals(cluster.member().node()) && m.type() == Member.Type.ACTIVE)
                .collect(Collectors.toSet());
            for (Member member : members) {
                this.replicas.add(new Replica(this.replicas.size(), member));
                this.commitTimes.add(System.currentTimeMillis());
            }

            this.quorum = (int) Math.floor((this.replicas.size() + 1) / 2.0);
            this.quorumIndex = quorum - 1;
        }

        /**
         * Triggers a commit.
         *
         * @return A completable future to be completed the next time entries are committed to a majority of the cluster.
         */
        private CompletableFuture<Long> commit() {
            executionContext.checkThread();
            if (replicas.isEmpty())
                return Futures.complete(null);

            if (commitFuture == null) {
                commitFuture = Futures.future();
                commitTime = System.currentTimeMillis();
                replicas.forEach((replica) -> {
                    try {
                        replica.commit();
                    } catch (IOException e) {
                        logger.error("error commit", e);
                    }
                });
                return commitFuture;
            } else if (nextCommitFuture == null) {
                nextCommitFuture = Futures.future();
                return nextCommitFuture;
            } else {
                return nextCommitFuture;
            }
        }

        /**
         * Registers a commit handler for the given commit index.
         *
         * @param index The index for which to register the handler.
         * @return A completable future to be completed once the given log index has been committed.
         */
        private CompletableFuture<Long> commit(long index) {
            executionContext.checkThread();
            if (index == 0)
                return commit();

            if (replicas.isEmpty()) {
                context.setCommitIndex(index);
                return Futures.complete(index);
            }

            return commitFutures.computeIfAbsent(index, i -> {
                replicas.forEach((replica) -> {
                    try {
                        replica.commit();
                    } catch (IOException e) {
                        logger.error("error commit", e);
                    }
                });
                return Futures.future();
            });
        }

        /**
         * Returns the last time a majority of the cluster was contacted.
         */
        private long commitTime() {
            executionContext.checkThread();
            Collections.sort(commitTimes, Collections.reverseOrder());
            return commitTimes.get(quorumIndex);
        }

        /**
         * Sets a commit time.
         */
        private void commitTime(int id) {
            executionContext.checkThread();
            commitTimes.set(id, System.currentTimeMillis());

            // Sort the list of commit times. Use the quorum index to get the last time the majority of the cluster
            // was contacted. If the current commitFuture's time is less than the commit time then trigger the
            // commit future and reset it to the next commit future.
            long commitTime = commitTime();
            if (commitFuture != null && this.commitTime <= commitTime) {
                commitFuture.complete(null);
                commitFuture = nextCommitFuture;
                nextCommitFuture = null;
                if (commitFuture != null) {
                    this.commitTime = System.currentTimeMillis();
                    replicas.forEach((replica) -> {
                        try {
                            replica.commit();
                        } catch (IOException e) {
                            logger.error("error commit", e);
                        }
                    });
                }
            }
        }

        /**
         * Checks whether any futures can be completed.
         */
        private void commitEntries() {
            executionContext.checkThread();
            // Sort the list of replicas, order by the last index that was replicated
            // to the replica. This will allow us to determine the median index
            // for all known replicated entries across all cluster members.
            Collections.sort(replicas, (o1, o2) -> Long.compare(o2.state.getMatchIndex() != 0 ? o2.state.getMatchIndex() : 0l, o1.state.getMatchIndex() != 0 ? o1.state.getMatchIndex() : 0l));

            // Set the current commit index as the median replicated index.
            // Since replicas is a list with zero based indexes, use the negation of
            // the required quorum size to get the index of the replica with the least
            // possible quorum replication. That replica's match index is the commit index.
            // Set the commit index. Once the commit index has been set we can run
            // all tasks up to the given commit.
            long commitIndex = replicas.get(quorumIndex).state.getMatchIndex();
            long globalIndex = replicas.get(replicas.size() - 1).state.getMatchIndex();
            if (commitIndex > 0) {
                context.setCommitIndex(commitIndex);
                context.setGlobalIndex(globalIndex);
                SortedMap<Long, CompletableFuture<Long>> futures = commitFutures.headMap(commitIndex, true);
                for (Map.Entry<Long, CompletableFuture<Long>> entry : futures.entrySet()) {
                    entry.getValue().complete(entry.getKey());
                }
                futures.clear();
            }
        }

        /**
         * Remote replica.
         */
        private class Replica {
            private final int id;
            private final Member member;
            private final MemberState state;
            private volatile boolean committing;

            private Replica(int id, Member member) {
                executionContext.checkThread();
                this.id = id;
                this.member = member;
                state = context.getMembers().getMember(member.node());
                state.setNextIndex(Math.max(state.getMatchIndex(), Math.max(context.getLog().lastIndex(), 1)));
            }

            /**
             * Triggers a commit for the replica.
             */
            private void commit() throws IOException {
                executionContext.checkThread();
                if (!committing) {
                    // If the log is empty then send an empty commit.
                    // If the next index hasn't yet been set then we send an empty commit first.
                    // If the next index is greater than the last index then send an empty commit.
                    if (context.getLog().isEmpty() || state.getNextIndex() > context.getLog().lastIndex()) {
                        emptyCommit();
                    } else {
                        entriesCommit();
                    }
                }
            }

            /**
             * Gets the previous index.
             */
            private long getPrevIndex() {
                executionContext.checkThread();
                return state.getNextIndex() - 1;
            }

            /**
             * Gets the previous entry.
             */
            private LogEntry getPrevEntry(long prevIndex) throws IOException {
                executionContext.checkThread();
                if (context.getLog().containsIndex(prevIndex)) {
                    return context.getLog().getEntry(prevIndex);
                }
                return null;
            }

            /**
             * Gets a list of entries to send.
             */
            @SuppressWarnings("unchecked")
            private List<LogEntry> getEntries(long prevIndex) throws IOException {
                executionContext.checkThread();
                long index;
                if (context.getLog().isEmpty()) {
                    return Collections.EMPTY_LIST;
                } else if (prevIndex != 0) {
                    index = prevIndex + 1;
                } else {
                    index = context.getLog().firstIndex();
                }

                List<LogEntry> entries = new ArrayList<>(1024);
                while (index <= context.getLog().lastIndex()) {
                    LogEntry entry = context.getLog().getEntry(index);
                    if (entry != null) {
                        entries.add(entry);
                    }
                    index++;
                }
                return entries;
            }

            /**
             * Performs an empty commit.
             */
            @SuppressWarnings("unchecked")
            private void emptyCommit() throws IOException {
                executionContext.checkThread();
                long prevIndex = getPrevIndex();
                LogEntry prevEntry = getPrevEntry(prevIndex);
                commit(prevIndex, prevEntry, Collections.EMPTY_LIST);
            }

            /**
             * Performs a commit with entries.
             */
            private void entriesCommit() throws IOException {
                executionContext.checkThread();
                logger.debug("entries commit for {}", member);
                long prevIndex = getPrevIndex();
                LogEntry prevEntry = getPrevEntry(prevIndex);
                List<LogEntry> entries = getEntries(prevIndex);
                commit(prevIndex, prevEntry, entries);
            }

            /**
             * Sends a commit message.
             */
            private void commit(long prevIndex, LogEntry prevEntry, List<LogEntry> entries) {
                executionContext.checkThread();
                AppendRequest request = AppendRequest.builder()
                    .setTerm(context.getTerm())
                    .setLeader(cluster.member().node())
                    .setLogIndex(prevIndex)
                    .setLogTerm(prevEntry != null ? prevEntry.term() : 0)
                    .setEntries(entries)
                    .setCommitIndex(context.getCommitIndex())
                    .setGlobalIndex(context.getGlobalIndex())
                    .build();

                committing = true;
                logger.debug("sent {} to {}", request, this.member);
                this.member.<AppendRequest, AppendResponse>send(request).whenCompleteAsync((response, error) -> {
                    committing = false;
                    if (error == null) {
                        logger.debug("received {} from {}", response, this.member);
                        if (response.status() == ResponseStatus.OK) {
                            // Update the commit time for the replica. This will cause heartbeat futures to be triggered.
                            commitTime(id);

                            // If replication succeeded then trigger commit futures.
                            if (response.succeeded()) {
                                updateMatchIndex(response);
                                updateNextIndex();

                                // If entries were committed to the replica then check commit indexes.
                                if (!entries.isEmpty()) {
                                    commitEntries();
                                }

                                // If there are more entries to send then attempt to send another commit.
                                if (hasMoreEntries()) {
                                    try {
                                        commit();
                                    } catch (IOException e) {
                                        logger.error("error commit", e);
                                    }
                                }
                            } else if (response.term() > context.getTerm()) {
                                transition(Raft.State.FOLLOWER);
                            } else {
                                resetMatchIndex(response);
                                resetNextIndex();

                                // If there are more entries to send then attempt to send another commit.
                                if (hasMoreEntries()) {
                                    try {
                                        commit();
                                    } catch (IOException e) {
                                        logger.error("error commit", e);
                                    }
                                }
                            }
                        } else if (response.term() > context.getTerm()) {
                            logger.info("received higher term from {}", this.member);
                            transition(Raft.State.FOLLOWER);
                        } else {
                            logger.warn("{}", response.error() != null ? response.error() : "");
                        }
                    } else {
                        logger.warn(error.getMessage());
                    }
                }, executionContext.executor());
            }

            private boolean hasMoreEntries() {
                executionContext.checkThread();
                return state.getNextIndex() < context.getLog().lastIndex();
            }

            private void updateMatchIndex(AppendResponse response) {
                executionContext.checkThread();
                // If the replica returned a valid match index then update the existing match index. Because the
                // replicator pipelines replication, we perform a MAX(matchIndex, logIndex) to get the true match index.
                state.setMatchIndex(Math.max(state.getMatchIndex(), response.logIndex()));
            }

            private void updateNextIndex() {
                executionContext.checkThread();
                // If the match index was set, update the next index to be greater than the match index if necessary.
                // Note that because of pipelining append requests, the next index can potentially be much larger than
                // the match index. We rely on the algorithm to reject invalid append requests.
                state.setNextIndex(Math.max(state.getNextIndex(), Math.max(state.getMatchIndex() + 1, 1)));
            }

            private void resetMatchIndex(AppendResponse response) {
                executionContext.checkThread();
                if (state.getMatchIndex() == 0) {
                    state.setMatchIndex(response.logIndex());
                } else if (response.logIndex() != 0) {
                    state.setMatchIndex(Math.max(state.getMatchIndex(), response.logIndex()));
                }
                logger.debug("reset match index for {} to {}", member, state.getMatchIndex());
            }

            private void resetNextIndex() {
                executionContext.checkThread();
                if (state.getMatchIndex() != 0) {
                    state.setNextIndex(state.getMatchIndex() + 1);
                } else {
                    state.setNextIndex(context.getLog().firstIndex());
                }
                logger.debug("reset next index for {} to {}", member, state.getNextIndex());
            }

        }
    }

}
