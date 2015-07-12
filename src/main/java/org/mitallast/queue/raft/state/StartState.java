package org.mitallast.queue.raft.state;

import com.google.inject.Inject;
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

import java.util.concurrent.CompletableFuture;

class StartState extends AbstractState {

    @Inject
    public StartState(Settings settings) {
        super(settings);
    }

    @Override
    public RaftStateType type() {
        return RaftStateType.START;
    }

    @Override
    public CompletableFuture<JoinResponse> join(JoinRequest request) {
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<RegisterResponse> register(RegisterRequest request) {
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<AppendResponse> append(AppendRequest request) {
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<VoteResponse> vote(VoteRequest request) {
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<CommandResponse> command(CommandRequest request) {
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<QueryResponse> query(QueryRequest request) {
        return Futures.completeExceptionally(new IllegalStateException("inactive state"));
    }

    @Override
    public void open() {
    }

    @Override
    public void close() {
    }
}
