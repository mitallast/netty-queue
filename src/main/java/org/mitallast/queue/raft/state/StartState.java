package org.mitallast.queue.raft.state;

import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
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
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.transport.TransportService;

import java.util.concurrent.CompletableFuture;

class StartState extends AbstractState {

    public StartState(Settings settings, RaftStateContext context, ExecutionContext executionContext, TransportService transportService) {
        super(settings, context, executionContext, transportService);
    }

    @Override
    public RaftStateType type() {
        return RaftStateType.START;
    }

    @Override
    public CompletableFuture<JoinResponse> join(JoinRequest request) {
        executionContext.checkThread();
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
        executionContext.checkThread();
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<RegisterResponse> register(RegisterRequest request) {
        executionContext.checkThread();
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
        executionContext.checkThread();
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<AppendResponse> append(AppendRequest request) {
        executionContext.checkThread();
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<VoteResponse> vote(VoteRequest request) {
        executionContext.checkThread();
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<CommandResponse> command(CommandRequest request) {
        executionContext.checkThread();
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<QueryResponse> query(QueryRequest request) {
        executionContext.checkThread();
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public void open() {
        executionContext.checkThread();
    }

    @Override
    public void close() {
        executionContext.checkThread();
    }
}
