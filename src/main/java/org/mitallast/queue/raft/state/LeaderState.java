package org.mitallast.queue.raft.state;

import com.google.inject.Inject;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.*;
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
import org.mitallast.queue.raft.cluster.ClusterService;
import org.mitallast.queue.raft.log.entry.*;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class LeaderState extends ActiveState {
    private final Replicator replicator = new Replicator();
    private final long maxEntries;
    private volatile ScheduledFuture<?> currentTimer;

    @Inject
    public LeaderState(
        Settings settings,
        RaftStateContext context,
        ExecutionContext executionContext,
        TransportService transportService,
        ClusterService clusterService
    ) {
        super(settings, context, executionContext, transportService, clusterService);
        maxEntries = componentSettings.getAsInt("max_entries", 10000);
    }

    @Override
    public RaftStateType type() {
        return RaftStateType.LEADER;
    }

    public synchronized void open() {
        executionContext.checkThread();
        // Schedule the initial entries commit to occur after the state is opened. Attempting any communication
        // within the open() method will result in a deadlock since RaftProtocol calls this method synchronously.
        // What is critical about this logic is that the heartbeat timer not be started until a NOOP entry has been committed.
        try {
            logger.info("await commit entries");
            commitEntries().whenCompleteAsync((result, error) -> {
                if (error == null) {
                    logger.info("await commit entries done");
                    startHeartbeatTimer();
                } else {
                    logger.warn("error commit entries", error);
                }
            }, executionContext.executor());
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
        context.setLeader(transportService.localNode());
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
        replicator.commit(index).whenCompleteAsync((resultIndex, error) -> {
            if (error == null) {
                try {
                    applyEntries(resultIndex);
                    future.complete(null);
                } catch (IOException e) {
                    future.completeExceptionally(e);
                }
            } else {
                logger.info("error commit, transitioning to follower", error);
                transition(RaftStateType.FOLLOWER);
            }
        }, executionContext.executor());
        return future;
    }

    /**
     * Applies all unapplied entries to the log.
     */
    private void applyEntries(long index) throws IOException {
        executionContext.checkThread();
        for (long lastApplied = Math.max(context.getStateMachine().getLastApplied(), context.getLog().firstIndex()); lastApplied <= index; lastApplied++) {
            RaftLogEntry entry = context.getLog().getEntry(lastApplied);
            if (entry != null) {
                context.getStateMachine().apply(entry).whenCompleteAsync((result, error) -> {
                    executionContext.checkThread();
                    if (error != null) {
                        logger.warn("application error occurred: {}", error);
                    }
                }, executionContext.executor());
            }
        }
    }

    /**
     * Starts heartbeating all cluster members.
     */
    private void startHeartbeatTimer() {
        // Set a timer that will be used to periodically synchronize with other nodes
        // in the cluster. This timer acts as a heartbeat to ensure this node remains
        // the leader.
        logger.info("start heartbeat timer");
        currentTimer = executionContext.scheduleAtFixedRate(this::heartbeatMembers, 0, context.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
    }

    private void stopHeartbeatTimer() {
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
    public CompletableFuture<AppendResponse> append(final AppendRequest request) {
        executionContext.checkThread();
        if (request.term() > context.getTerm()) {
            return super.append(request);
        } else if (request.term() < context.getTerm()) {
            return Futures.complete(AppendResponse.builder()
                .setTerm(context.getTerm())
                .setSucceeded(false)
                .setLogIndex(context.getLog().lastIndex())
                .build());
        } else {
            logger.info("received append request, transitioning to follower");
            transition(RaftStateType.FOLLOWER);
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

        CommandEntry entry = CommandEntry.builder()
            .setTerm(term)
            .setIndex(context.getLog().nextIndex())
            .setSession(request.session())
            .setRequest(request.request())
            .setResponse(request.response())
            .setTimestamp(timestamp)
            .setCommand(command)
            .build();
        try {
            index = context.getLog().appendEntry(entry);
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }

        CompletableFuture<CommandResponse> future = Futures.future();
        replicator.commit(index).whenCompleteAsync((commitIndex, commitError) -> {

            if (commitError == null) {
                if (entry != null) {
                    context.getStateMachine().apply(entry).whenCompleteAsync((result, error) -> {
                        executionContext.checkThread();
                        if (error == null) {
                            future.complete(CommandResponse.builder()
                                .setResult(result)
                                .build());
                        } else {
                            logger.error("command error", error);
                            future.complete(CommandResponse.builder()
                                .setError(new InternalException())
                                .build());
                        }
                    }, executionContext.executor());
                } else {
                    future.complete(CommandResponse.builder()
                        .setResult(null)
                        .build());
                }
            } else {
                logger.error("error apply entry", commitError);
                future.complete(CommandResponse.builder()
                    .setError(new WriteException())
                    .build());
            }
        }, executionContext.executor());
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
        replicator.commit().whenCompleteAsync((commitIndex, commitError) -> {
            if (commitError == null) {
                applyQuery(entry, future);
            } else {
                logger.error("commitError error", commitError);
                future.complete(QueryResponse.builder()
                    .setError(new ReadException())
                    .build());
            }
        }, executionContext.executor());
        return future;
    }

    /**
     * Applies a query to the state machine.
     */
    private CompletableFuture<QueryResponse> applyQuery(QueryEntry entry, CompletableFuture<QueryResponse> future) {
        executionContext.checkThread();
        long version = context.getStateMachine().getLastApplied();
        context.getStateMachine().apply(entry).whenCompleteAsync((result, error) -> {
            if (error == null) {
                future.complete(QueryResponse.builder()
                    .setVersion(version)
                    .setResult(result)
                    .build());
            } else if (error instanceof ApplicationException) {
                logger.error("application error", error);
                future.complete(QueryResponse.builder()
                    .setError(new ApplicationException())
                    .build());
            } else {
                logger.error("internal error", error);
                future.complete(QueryResponse.builder()
                    .setError(new InternalException())
                    .build());
            }
        }, executionContext.executor());
        return future;
    }

    @Override
    public CompletableFuture<RegisterResponse> register(RegisterRequest request) {
        executionContext.checkThread();
        final long timestamp = System.currentTimeMillis();
        final long index;

        RegisterEntry entry = RegisterEntry.builder()
            .setTerm(context.getTerm())
            .setIndex(context.getLog().nextIndex())
            .setTimestamp(timestamp)
            .build();

        try {
            logger.info("register entry append {}", entry);
            index = context.getLog().appendEntry(entry);
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }

        CompletableFuture<RegisterResponse> future = Futures.future();
        replicator.commit(index).whenCompleteAsync((commitIndex, commitError) -> {
            if (commitError == null) {
                logger.info("register entry apply {}", entry);
                context.getStateMachine().apply(entry).whenCompleteAsync((sessionId, sessionError) -> {
                    executionContext.checkThread();
                    logger.info("register entry {} session id {} error {}", entry, sessionId, sessionError);
                    if (sessionError == null) {
                        try {
                            future.complete(RegisterResponse.builder()
                                .setLeader(transportService.localNode())
                                .setTerm(context.getTerm())
                                .setSession(sessionId)
                                .setMembers(clusterService.nodes())
                                .build());
                            logger.info("register entry response: {}", future);
                        } catch (Throwable e) {
                            logger.info("register entry error", e);
                            future.completeExceptionally(e);
                        }
                    } else if (sessionError instanceof ApplicationException) {
                        logger.error("application error", sessionError);
                        future.complete(RegisterResponse.builder()
                            .setError(new ApplicationException())
                            .build());
                    } else {
                        logger.error("session error", sessionError);
                        future.complete(RegisterResponse.builder()
                            .setError(new InternalException())
                            .build());
                    }
                }, executionContext.executor());
            } else {
                logger.error("commit error", commitError);
                future.complete(RegisterResponse.builder()
                    .setError(new ProtocolException())
                    .build());
            }
        }, executionContext.executor());
        return future;
    }

    @Override
    public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
        executionContext.checkThread();
        final long timestamp = System.currentTimeMillis();
        final long index;

        KeepAliveEntry entry = KeepAliveEntry.builder()
            .setTerm(context.getTerm())
            .setIndex(context.getLog().nextIndex())
            .setSession(request.session())
            .setTimestamp(timestamp)
            .build();

        try {
            index = context.getLog().appendEntry(entry);
            logger.debug("appended session entry to log at index {}", index);
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }

        CompletableFuture<KeepAliveResponse> future = Futures.future();
        replicator.commit(index).whenCompleteAsync((commitIndex, commitError) -> {
            if (commitError == null) {
                long version = context.getStateMachine().getLastApplied();
                context.getStateMachine().apply(entry).whenCompleteAsync((sessionResult, sessionError) -> {
                    executionContext.checkThread();
                    if (sessionError == null) {
                        try {
                            future.complete(KeepAliveResponse.builder()
                                .setLeader(transportService.localNode())
                                .setTerm(context.getTerm())
                                .setVersion(version)
                                .setMembers(clusterService.nodes())
                                .build());
                        } catch (Throwable e) {
                            future.completeExceptionally(e);
                        }
                    } else if (sessionError instanceof ApplicationException) {
                        logger.error("application error", sessionError);
                        future.complete(KeepAliveResponse.builder()
                            .setError(new ApplicationException())
                            .build());
                    } else {
                        logger.error("session error", sessionError);
                        future.complete(KeepAliveResponse.builder()
                            .setError(new InternalException())
                            .build());
                    }
                }, executionContext.executor());
            } else {
                logger.error("commit error", commitError);
                future.complete(KeepAliveResponse.builder()
                    .setError(new ProtocolException())
                    .build());
            }
        }, executionContext.executor());
        return future;
    }

    @Override
    public CompletableFuture<JoinResponse> join(JoinRequest request) {
        executionContext.checkThread();
        final long index;

        JoinEntry entry = JoinEntry.builder()
            .setTerm(context.getTerm())
            .setIndex(context.getLog().nextIndex())
            .setMember(request.member())
            .build();
        try {
            index = context.getLog().appendEntry(entry);
            logger.debug("appended {}", entry);
        } catch (Throwable e) {
            return Futures.completeExceptionally(e);
        }

        CompletableFuture<JoinResponse> future = Futures.future();
        replicator.commit(index).whenCompleteAsync((commitIndex, commitError) -> {
            if (commitError == null) {
                context.getStateMachine().apply(entry).whenCompleteAsync((sessionId, sessionError) -> {
                    executionContext.checkThread();
                    if (sessionError == null) {
                        try {
                            future.complete(JoinResponse.builder()
                                .setLeader(transportService.localNode())
                                .setTerm(context.getTerm())
                                .build());
                        } catch (Throwable e) {
                            future.completeExceptionally(e);
                        }
                    } else {
                        logger.error("session error", sessionError);
                        future.complete(JoinResponse.builder()
                            .setError(new InternalException())
                            .build());
                    }
                }, executionContext.executor());
            } else {
                logger.error("commit error", commitError);
                future.complete(JoinResponse.builder()
                    .setError(new ProtocolException())
                    .build());
            }
        }, executionContext.executor());
        return future;
    }

    @Override
    public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
        executionContext.checkThread();
        final long index;

        LeaveEntry entry = LeaveEntry.builder()
            .setTerm(context.getTerm())
            .setIndex(context.getLog().nextIndex())
            .setMember(request.member())
            .build();

        try {
            index = context.getLog().appendEntry(entry);
            logger.debug("appended {}", entry);
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }

        CompletableFuture<LeaveResponse> future = Futures.future();
        replicator.commit(index).whenCompleteAsync((commitIndex, commitError) -> {
            if (commitError == null) {
                context.getStateMachine().apply(entry).whenCompleteAsync((sessionId, sessionError) -> {
                    executionContext.checkThread();
                    if (sessionError == null) {
                        future.complete(LeaveResponse.builder()
                            .build());
                    } else {
                        logger.error("session error", sessionError);
                        future.complete(LeaveResponse.builder()
                            .setError(new InternalException())
                            .build());
                    }
                }, executionContext.executor());
            } else {
                logger.error("commit error", commitError);
                future.complete(LeaveResponse.builder()
                    .setError(new ProtocolException())
                    .build());
            }
        }, executionContext.executor());
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
            clusterService.members().stream()
                .filter(state -> !state.getNode().equals(transportService.localNode()))
                .forEach(state -> {
                    replicas.add(new Replica(this.replicas.size(), state));
                    commitTimes.add(System.currentTimeMillis());
                });

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
            if (replicas.isEmpty()) {
                logger.debug("no replicas found, return completed future");
                return Futures.complete(null);
            }

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
            logger.debug("commit {}", index);
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
            private final MemberState state;
            private volatile boolean committing;

            private Replica(int id, MemberState state) {
                executionContext.checkThread();
                this.id = id;
                this.state = state;
                this.state.setNextIndex(Math.max(state.getMatchIndex(), Math.max(context.getLog().lastIndex(), 1)));
            }

            /**
             * Triggers a commit for the replica.
             */
            private void commit() throws IOException {
                logger.debug("[replica {}] commit", state.getNode());
                executionContext.checkThread();
                if (!committing) {
                    // If the log is empty then send an empty commit.
                    // If the next index hasn't yet been set then we send an empty commit first.
                    // If the next index is greater than the last index then send an empty commit.
                    if (state.getNextIndex() > context.getLog().lastIndex()) {
                        emptyCommit();
                    } else {
                        entriesCommit();
                    }
                }
                logger.debug("[replica {}] commit done", state.getNode());
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
            private RaftLogEntry getPrevEntry(long prevIndex) throws IOException {
                executionContext.checkThread();
                if (context.getLog().containsIndex(prevIndex)) {
                    return context.getLog().getEntry(prevIndex);
                }
                return null;
            }

            /**
             * Gets a list of entries to send.
             */
            private List<RaftLogEntry> getEntries(long prevIndex) throws IOException {
                executionContext.checkThread();
                long index = Math.max(context.getLog().firstIndex(), prevIndex + 1);

                List<RaftLogEntry> entries = new ArrayList<>();
                while (index <= context.getLog().lastIndex() && entries.size() < maxEntries) {
                    RaftLogEntry entry = context.getLog().getEntry(index);
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
            private void emptyCommit() throws IOException {
                logger.debug("[replica {}] empty commit", state.getNode());
                executionContext.checkThread();
                long prevIndex = getPrevIndex();
                RaftLogEntry prevEntry = getPrevEntry(prevIndex);
                commit(prevIndex, prevEntry, Collections.emptyList());
            }

            /**
             * Performs a commit with entries.
             */
            private void entriesCommit() throws IOException {
                executionContext.checkThread();
                logger.debug("[replica {}] entries commit", state.getNode());
                long prevIndex = getPrevIndex();
                RaftLogEntry prevEntry = getPrevEntry(prevIndex);
                List<RaftLogEntry> entries = getEntries(prevIndex);
                commit(prevIndex, prevEntry, entries);
            }

            /**
             * Sends a commit message.
             */
            private void commit(long prevIndex, RaftLogEntry prevEntry, List<RaftLogEntry> entries) {
                executionContext.checkThread();
                AppendRequest request = AppendRequest.builder()
                    .setTerm(context.getTerm())
                    .setLeader(transportService.localNode())
                    .setLogIndex(prevIndex)
                    .setLogTerm(prevEntry != null ? prevEntry.term() : 0)
                    .setEntries(entries)
                    .setCommitIndex(context.getCommitIndex())
                    .setGlobalIndex(context.getGlobalIndex())
                    .build();

                committing = true;
                logger.debug("[replica {}] replicate {}", state.getNode(), request);
                transportService.client(state.getNode().address()).<AppendRequest, AppendResponse>send(request).whenCompleteAsync((response, error) -> {
                    committing = false;
                    if (error == null) {
                        logger.debug("[replica {}] received {}", state.getNode(), response);
                        if (response.term() > context.getTerm()) {
                            logger.info("[replica {}] received higher term from {}, transitioning to follower", state.getNode());
                            transition(RaftStateType.FOLLOWER);
                        } else {
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
                                        logger.error("[replica {}] error commit", state.getNode(), e);
                                    }
                                }
                            } else if (response.term() > context.getTerm()) {
                                logger.info("[replica {}] received greater term append response, transitioning to follower", state.getNode());
                                transition(RaftStateType.FOLLOWER);
                            } else {
                                resetMatchIndex(response);
                                resetNextIndex();

                                // If there are more entries to send then attempt to send another commit.
                                if (hasMoreEntries()) {
                                    try {
                                        commit();
                                    } catch (IOException e) {
                                        logger.error("[replica {}] error commit", state.getNode(), e);
                                    }
                                }
                            }
                        }
                    } else {
                        logger.warn("[replica {}] error send {}", state.getNode(), error.getMessage());
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
                logger.debug("[replica {}] reset match index to {}", state.getNode(), state.getMatchIndex());
            }

            private void resetNextIndex() {
                executionContext.checkThread();
                if (state.getMatchIndex() != 0) {
                    state.setNextIndex(state.getMatchIndex() + 1);
                } else {
                    state.setNextIndex(context.getLog().firstIndex());
                }
                logger.debug("[replica {}] reset next index to {}", state.getNode(), state.getNextIndex());
            }

        }
    }

}
