package org.mitallast.queue.raft.state;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.ApplicationException;
import org.mitallast.queue.raft.ConsistencyLevel;
import org.mitallast.queue.raft.NoLeaderException;
import org.mitallast.queue.raft.action.query.QueryRequest;
import org.mitallast.queue.raft.action.query.QueryResponse;
import org.mitallast.queue.raft.action.vote.VoteRequest;
import org.mitallast.queue.raft.action.vote.VoteResponse;
import org.mitallast.queue.raft.cluster.TransportCluster;
import org.mitallast.queue.raft.log.entry.LogEntry;
import org.mitallast.queue.raft.log.entry.QueryEntry;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

abstract class ActiveState extends PassiveState {

    protected ActiveState(Settings settings, RaftStateContext context, ExecutionContext executionContext, TransportCluster cluster, TransportService transportService) {
        super(settings, context, executionContext, cluster, transportService);
    }

    @Override
    protected void transition(RaftStateType state) {
        executionContext.checkThread();
        switch (state) {
            case START:
                context.transition(StartState.class);
                break;
            case PASSIVE:
                context.transition(PassiveState.class);
                break;
            case FOLLOWER:
                context.transition(FollowerState.class);
                break;
            case CANDIDATE:
                context.transition(CandidateState.class);
                break;
            case LEADER:
                context.transition(LeaderState.class);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public CompletableFuture<VoteResponse> vote(VoteRequest request) {
        executionContext.checkThread();
        try {
            return Futures.complete(handleVote(request));
        } catch (IOException e) {
            return Futures.completeExceptionally(e);
        }
    }

    protected VoteResponse handleVote(VoteRequest request) throws IOException {
        executionContext.checkThread();
        // If the request indicates a term that is greater than the current term then
        // assign that term and leader to the current context and step down as leader.
        if (request.term() > context.getTerm()) {
            context.setTerm(request.term());
        }

        // If the request term is not as great as the current context term then don't
        // vote for the candidate. We want to vote for candidates that are at least
        // as up to date as us.
        if (request.term() < context.getTerm()) {
            logger.info("rejected {}: candidate's term is less than the current term", request);
            return VoteResponse.builder()
                .setTerm(context.getTerm())
                .setVoted(false)
                .build();
        }
        // If the requesting candidate is our self then always vote for our self. Votes
        // for self are done by calling the local node. Note that this obviously
        // doesn't make sense for a leader.
        else if (transportService.localNode().equals(request.candidate())) {
            context.setLastVotedFor(transportService.localNode());
            logger.info("accepted {}: candidate is the local member", request);
            return VoteResponse.builder()
                .setTerm(context.getTerm())
                .setVoted(true)
                .build();
        }
        // If the requesting candidate is not a known member of the cluster (to this
        // node) then don't vote for it. Only vote for candidates that we know about.
        else if (!context.getMembers().getMembers().stream().map(MemberState::getNode).collect(Collectors.toSet()).contains(request.candidate())) {
            logger.info("rejected {}: candidate is not known to the local member: {}", request, context.getMembers().nodes());
            return VoteResponse.builder()
                .setTerm(context.getTerm())
                .setVoted(false)
                .build();
        }
        // If we've already voted for someone else then don't vote again.
        else if (context.getLastVotedFor() == null || context.getLastVotedFor() == request.candidate()) {
            if (logUpToDate(request.logIndex(), request.logTerm(), request)) {
                context.setLastVotedFor(request.candidate());
                return VoteResponse.builder()
                    .setTerm(context.getTerm())
                    .setVoted(true)
                    .build();
            } else {
                return VoteResponse.builder()
                    .setTerm(context.getTerm())
                    .setVoted(false)
                    .build();
            }
        }
        // In this case, we've already voted for someone else.
        else {
            logger.info("rejected {}: already voted for {}", request, context.getLastVotedFor());
            return VoteResponse.builder()
                .setTerm(context.getTerm())
                .setVoted(false)
                .build();
        }
    }

    private boolean logUpToDate(long index, long term, ActionRequest request) throws IOException {
        executionContext.checkThread();
        // If the log is empty then vote for the candidate.
        if (context.getLog().isEmpty()) {
            logger.info("accepted {}: candidate's log is up-to-date", request);
            return true;
        } else {
            // Otherwise, load the last entry in the log. The last entry should be
            // at least as up to date as the candidates entry and term.
            long lastIndex = context.getLog().lastIndex();
            LogEntry entry = context.getLog().getEntry(lastIndex);
            if (entry == null) {
                logger.info("accepted {}: candidate's log is up-to-date", request);
                return true;
            }

            if (index != 0 && index >= lastIndex) {
                if (term >= entry.term()) {
                    logger.info("accepted {}: candidate's log is up-to-date", request);
                    return true;
                } else {
                    logger.info("rejected {}: candidate's last log term ({}) is in conflict with local log ({})", request, term, entry.term());
                    return false;
                }
            } else {
                logger.info("rejected {}: candidate's last log entry ({}) is at a lower index than the local log ({})", request, index, lastIndex);
                return false;
            }
        }
    }

    @Override
    public CompletableFuture<QueryResponse> query(QueryRequest request) {
        executionContext.checkThread();
        if (request.query().consistency() == ConsistencyLevel.SEQUENTIAL) {
            return querySequential(request);
        } else if (request.query().consistency() == ConsistencyLevel.SERIALIZABLE) {
            return querySerializable(request);
        } else {
            return queryForward(request);
        }
    }

    private CompletableFuture<QueryResponse> queryForward(QueryRequest request) {
        executionContext.checkThread();
        if (context.getLeader() == null) {
            logger.error("no leader error");
            return Futures.complete(QueryResponse.builder()
                .setError(new NoLeaderException())
                .build());
        }
        return transportService.client(context.getLeader().address()).send(request);
    }

    private CompletableFuture<QueryResponse> querySequential(QueryRequest request) {
        executionContext.checkThread();
        // If the commit index is not in the log then we've fallen too far behind the leader to perform a query.
        // Forward the request to the leader.
        if (context.getLog().lastIndex() < context.getCommitIndex()) {
            logger.info("state appears to be out of sync, forwarding query to leader");
            return queryForward(request);
        }

        CompletableFuture<QueryResponse> future = Futures.future();
        QueryEntry entry = QueryEntry.builder()
            .setIndex(context.getCommitIndex())
            .setTerm(context.getTerm())
            .setTimestamp(System.currentTimeMillis())
            .setVersion(request.version())
            .setSession(request.session())
            .setQuery(request.query())
            .build();

        long version = Math.max(context.getStateMachine().getLastApplied(), request.version());
        context.getStateMachine().apply(entry).whenComplete((result, error) -> {
            executionContext.checkThread();
            if (error == null) {
                future.complete(QueryResponse.builder()
                    .setVersion(version)
                    .setResult(result)
                    .build());
            } else {
                logger.error("application error", error);
                future.complete(QueryResponse.builder()
                    .setError(new ApplicationException())
                    .build());
            }
        });
        return future;
    }

    private CompletableFuture<QueryResponse> querySerializable(QueryRequest request) {
        executionContext.checkThread();
        // If the commit index is not in the log then we've fallen too far behind the leader to perform a query.
        // Forward the request to the leader.
        if (context.getLog().lastIndex() < context.getCommitIndex()) {
            logger.info("state appears to be out of sync, forwarding query to leader");
            return queryForward(request);
        }

        CompletableFuture<QueryResponse> future = Futures.future();
        QueryEntry entry = QueryEntry.builder()
            .setIndex(context.getCommitIndex())
            .setTerm(context.getTerm())
            .setTimestamp(System.currentTimeMillis())
            .setVersion(0)
            .setSession(request.session())
            .setQuery(request.query())
            .build();

        long version = context.getStateMachine().getLastApplied();
        context.getStateMachine().apply(entry).whenComplete((result, error) -> {
            executionContext.checkThread();
            if (error == null) {
                future.complete(QueryResponse.builder()
                    .setVersion(version)
                    .setResult(result)
                    .build());
            } else {
                logger.error("application error", error);
                future.complete(QueryResponse.builder()
                    .setError(new ApplicationException())
                    .build());
            }
        });
        return future;
    }

}
