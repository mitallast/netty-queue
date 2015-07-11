package org.mitallast.queue.raft.state;

import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.IllegalMemberStateException;
import org.mitallast.queue.raft.NoLeaderException;
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
import org.mitallast.queue.raft.cluster.TransportCluster;
import org.mitallast.queue.raft.log.entry.LogEntry;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class PassiveState extends AbstractState {
    protected volatile boolean transition;

    public PassiveState(Settings settings, RaftStateContext context, ExecutionContext executionContext, TransportCluster cluster, TransportService transportService) {
        super(settings, context, executionContext, cluster, transportService);
    }

    @Override
    public RaftStateType type() {
        return RaftStateType.PASSIVE;
    }

    protected void transition(RaftStateType state) {
        executionContext.checkThread();
        // Do not allow the PASSIVE state to transition.
    }

    @Override
    public CompletableFuture<AppendResponse> append(final AppendRequest request) {
        executionContext.checkThread();
        CompletableFuture<AppendResponse> future;
        try {
            future = Futures.complete(handleAppend(request));
        } catch (IOException e) {
            future = Futures.completeExceptionally(e);
        }
        // If a transition is required then transition back to the follower state.
        // If the node is already a follower then the transition will be ignored.
        if (transition) {
            transition(RaftStateType.FOLLOWER);
            transition = false;
        }
        return future;
    }

    private AppendResponse handleAppend(AppendRequest request) throws IOException {
        executionContext.checkThread();
        // If the request indicates a term that is greater than the current term then
        // assign that term and leader to the current context and step down as leader.
        if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == null)) {
            context.setTerm(request.term());
            context.setLeader(request.leader());
            transition = true;
        }

        // If the request term is less than the current term then immediately
        // reply false and return our current term. The leader will receive
        // the updated term and step down.
        if (request.term() < context.getTerm()) {
            logger.warn("rejected {}: request term is less than the current term ({})", request, context.getTerm());
            return AppendResponse.builder()
                .setTerm(context.getTerm())
                .setSucceeded(false)
                .setLogIndex(context.getLog().lastIndex())
                .build();
        } else if (request.logIndex() != 0 && request.logTerm() != 0) {
            return doCheckPreviousEntry(request);
        } else {
            return doAppendEntries(request);
        }
    }

    private AppendResponse doCheckPreviousEntry(AppendRequest request) throws IOException {
        executionContext.checkThread();
        if (request.logIndex() != 0 && context.getLog().isEmpty()) {
            logger.warn("rejected {}: previous index ({}) is greater than the local log's last index ({})", request, request.logIndex(), context.getLog().lastIndex());
            return AppendResponse.builder()
                .setTerm(context.getTerm())
                .setSucceeded(false)
                .setLogIndex(context.getLog().lastIndex())
                .build();
        } else if (request.logIndex() != 0 && context.getLog().lastIndex() != 0 && request.logIndex() > context.getLog().lastIndex()) {
            logger.warn("rejected {}: previous index ({}) is greater than the local log's last index ({})", request, request.logIndex(), context.getLog().lastIndex());
            return AppendResponse.builder()
                .setTerm(context.getTerm())
                .setSucceeded(false)
                .setLogIndex(context.getLog().lastIndex())
                .build();
        }

        // If the previous entry term doesn't match the local previous term then reject the request.
        LogEntry entry = context.getLog().getEntry(request.logIndex());
        if (entry == null || entry.term() != request.logTerm()) {
            logger.warn("rejected {}: request log term does not match local log term {} for the same entry", request, entry != null ? entry.term() : null);
            return AppendResponse.builder()
                .setTerm(context.getTerm())
                .setSucceeded(false)
                .setLogIndex(request.logIndex() <= context.getLog().lastIndex() ? request.logIndex() - 1 : context.getLog().lastIndex())
                .build();
        } else {
            return doAppendEntries(request);
        }
    }

    private AppendResponse doAppendEntries(AppendRequest request) throws IOException {
        executionContext.checkThread();
        // If the log contains entries after the request's previous log index
        // then remove those entries to be replaced by the request entries.
        if (!request.entries().isEmpty()) {

            // Iterate through request entries and append them to the log.
            for (LogEntry entry : request.entries()) {
                // If the entry index is greater than the last log index, skip missing entries.
                if (!context.getLog().containsIndex(entry.index())) {
                    context.getLog().skip(entry.index() - context.getLog().lastIndex() - 1).appendEntry(entry);
                    logger.debug("appended {} to log at index {}", entry, entry.index());
                } else {
                    // Compare the term of the received entry with the matching entry in the log.
                    LogEntry match = context.getLog().getEntry(entry.index());
                    if (match != null) {
                        if (entry.term() != match.term()) {
                            // We found an invalid entry in the log. Remove the invalid entry and append the new entry.
                            // If appending to the log fails, apply commits and reply false to the append request.
                            logger.warn("appended entry term does not match local log, removing incorrect entries");
                            context.getLog().truncate(entry.index() - 1);
                            context.getLog().appendEntry(entry);
                            logger.debug("appended {} to log at index {}", entry, entry.index());
                        }
                    } else {
                        context.getLog().truncate(entry.index() - 1).appendEntry(entry);
                        logger.debug("appended {} to log at index {}", entry, entry.index());
                    }
                }
            }
        }

        // If we've made it this far, apply commits and send a successful response.
        executionContext.execute(() -> {
            try {
                applyCommits(request.commitIndex()).thenRun(() -> applyIndex(request.globalIndex()));
            } catch (IOException e) {
                logger.error("error apply commits", e);
            }
        });

        return AppendResponse.builder()
            .setTerm(context.getTerm())
            .setSucceeded(true)
            .setLogIndex(context.getLog().lastIndex())
            .build();
    }

    private CompletableFuture<Void> applyCommits(long commitIndex) throws IOException {
        executionContext.checkThread();
        // Set the commit index, ensuring that the index cannot be decreased.
        context.setCommitIndex(Math.max(context.getCommitIndex(), commitIndex));

        // The entries to be applied to the state machine are the difference between min(lastIndex, commitIndex) and lastApplied.
        long lastIndex = context.getLog().lastIndex();
        long lastApplied = context.getStateMachine().getLastApplied();

        long effectiveIndex = Math.min(lastIndex, context.getCommitIndex());

        // If the effective commit index is greater than the last index applied to the state machine then apply remaining entries.
        if (effectiveIndex > lastApplied) {
            long entriesToApply = effectiveIndex - lastApplied;
            logger.debug("applying {} commits", entriesToApply);

            // Rather than composing all futures into a single future, use a counter to count completions in order to preserve memory.
            AtomicLong counter = new AtomicLong();
            CompletableFuture<Void> future = Futures.future();

            for (long i = lastApplied + 1; i <= effectiveIndex; i++) {
                LogEntry entry = context.getLog().getEntry(i);
                if (entry != null) {
                    applyEntry(entry).whenComplete((result, error) -> {
                        executionContext.checkThread();
                        if (error != null) {
                            logger.debug("application error occurred", error);
                        }
                        if (counter.incrementAndGet() == entriesToApply) {
                            future.complete(null);
                        }
                    });
                }
            }
        }
        return Futures.complete(null);
    }

    protected CompletableFuture<?> applyEntry(LogEntry entry) {
        executionContext.checkThread();
        return context.getStateMachine().apply(entry);
    }

    protected void applyIndex(long globalIndex) {
        executionContext.checkThread();
        if (globalIndex > 0) {
            context.setGlobalIndex(globalIndex);
        }
    }

    @Override
    public CompletableFuture<VoteResponse> vote(VoteRequest request) {
        executionContext.checkThread();
        return Futures.complete(VoteResponse.builder()
            .setError(new IllegalMemberStateException())
            .build());
    }

    @Override
    public CompletableFuture<CommandResponse> command(CommandRequest request) {
        executionContext.checkThread();
        if (context.getLeader() == null) {
            return Futures.complete(CommandResponse.builder()
                .setError(new NoLeaderException())
                .build());
        } else {
            return transportService.client(context.getLeader().address()).send(request);
        }
    }

    @Override
    public CompletableFuture<QueryResponse> query(QueryRequest request) {
        executionContext.checkThread();
        if (context.getLeader() == null) {
            return Futures.complete(QueryResponse.builder()
                .setError(new NoLeaderException())
                .build());
        } else {
            return transportService.client(context.getLeader().address()).send(request);
        }
    }

    @Override
    public void open() {
        executionContext.checkThread();
    }

    @Override
    public void close() {
        executionContext.checkThread();
    }

    @Override
    public CompletableFuture<RegisterResponse> register(RegisterRequest request) {
        executionContext.checkThread();
        if (context.getLeader() == null) {
            return Futures.complete(RegisterResponse.builder()
                .setError(new NoLeaderException())
                .build());
        } else {
            return transportService.client(context.getLeader().address()).send(request);
        }
    }

    @Override
    public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
        executionContext.checkThread();
        if (context.getLeader() == null) {
            return Futures.complete(KeepAliveResponse.builder()
                .setError(new NoLeaderException())
                .build());
        } else {
            return transportService.client(context.getLeader().address()).send(request);
        }
    }

    @Override
    public CompletableFuture<JoinResponse> join(JoinRequest request) {
        executionContext.checkThread();
        if (context.getLeader() == null) {
            return Futures.complete(JoinResponse.builder()
                .setError(new NoLeaderException())
                .build());
        } else {
            return transportService.client(context.getLeader().address()).send(request);
        }
    }

    @Override
    public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
        executionContext.checkThread();
        if (context.getLeader() == null) {
            return Futures.complete(LeaveResponse.builder()
                .setError(new NoLeaderException())
                .build());
        } else {
            return transportService.client(context.getLeader().address()).send(request);
        }
    }
}
