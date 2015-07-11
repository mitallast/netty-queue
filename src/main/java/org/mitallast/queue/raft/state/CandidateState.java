package org.mitallast.queue.raft.state;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.action.append.AppendRequest;
import org.mitallast.queue.raft.action.append.AppendResponse;
import org.mitallast.queue.raft.action.vote.VoteRequest;
import org.mitallast.queue.raft.action.vote.VoteResponse;
import org.mitallast.queue.raft.cluster.TransportCluster;
import org.mitallast.queue.raft.log.entry.LogEntry;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.raft.util.Quorum;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class CandidateState extends ActiveState {
    private final Random random = new Random();
    private volatile Quorum quorum;
    private volatile ScheduledFuture<?> currentTimer;

    public CandidateState(Settings settings, RaftStateContext context, ExecutionContext executionContext, TransportCluster cluster, TransportService transportService) {
        super(settings, context, executionContext, cluster, transportService);
    }

    @Override
    public RaftStateType type() {
        return RaftStateType.CANDIDATE;
    }

    public synchronized void open() {
        startElection();
    }

    private void startElection() {
        executionContext.checkThread();
        logger.info("starting election");
        try {
            sendVoteRequests();
        } catch (IOException e) {
            logger.info("error send vote requests", e);
        }
    }

    private void sendVoteRequests() throws IOException {
        executionContext.checkThread();
        // Cancel the current timer task and purge the election timer of cancelled tasks.
        if (currentTimer != null) {
            currentTimer.cancel(false);
        }

        // When the election timer is reset, increment the current term and
        // restart the election.
        context.setTerm(context.getTerm() + 1);

        long delay = context.getElectionTimeout() + (random.nextInt((int) context.getElectionTimeout()) % context.getElectionTimeout());
        currentTimer = executionContext.schedule(() -> {
            // When the election times out, clear the previous majority vote
            // check and restart the election.
            logger.info("election timed out");
            if (quorum != null) {
                quorum.cancel();
                quorum = null;
            }
            try {
                sendVoteRequests();
            } catch (IOException e) {
                logger.info("error send vote requests", e);
            }
            logger.info("restarted election");
        }, delay, TimeUnit.MILLISECONDS);

        final AtomicBoolean complete = new AtomicBoolean();
        ImmutableList<MemberState> votingMembers = context.getMembers().getActiveMembers();

        // Send vote requests to all nodes. The vote request that is sent
        // to this node will be automatically successful.
        // First check if the quorum is null. If the quorum isn't null then that
        // indicates that another vote is already going on.
        final Quorum quorum = new Quorum((int) Math.floor(votingMembers.size() / 2.0) + 1, (elected) -> {
            complete.set(true);
            if (elected) {
                transition(RaftStateType.LEADER);
            }
        });

        // First, load the last log entry to get its term. We load the entry
        // by its index since the index is required by the protocol.
        long lastIndex = context.getLog().lastIndex();
        LogEntry lastEntry = lastIndex != 0 ? context.getLog().getEntry(lastIndex) : null;

        // Once we got the last log term, iterate through each current member
        // of the cluster and vote each member for a vote.
        logger.info("requesting votes from {}", votingMembers);
        final long lastTerm = lastEntry != null ? lastEntry.term() : 0;
        for (MemberState member : votingMembers) {
            logger.info("requesting vote from {} for term {}", member, context.getTerm());
            VoteRequest request = VoteRequest.builder()
                .setTerm(context.getTerm())
                .setCandidate(transportService.localNode())
                .setLogIndex(lastIndex)
                .setLogTerm(lastTerm)
                .build();

            transportService.client(member.getNode().address()).<VoteRequest, VoteResponse>send(request).whenCompleteAsync((response, error) -> {
                if (!complete.get()) {
                    if (error != null) {
                        logger.warn(error.getMessage());
                        quorum.fail();
                    } else if (response.term() > context.getTerm()) {
                        logger.info("received greater term from {}", member);
                        context.setTerm(response.term());
                        complete.set(true);
                        transition(RaftStateType.FOLLOWER);
                    } else if (!response.voted()) {
                        logger.info("received rejected vote from {}", member);
                        quorum.fail();
                    } else if (response.term() != context.getTerm()) {
                        logger.info("received successful vote for a different term from {}", member);
                        quorum.fail();
                    } else {
                        logger.info("received successful vote from {}", member);
                        quorum.succeed();
                    }
                }
            }, executionContext.executor());
        }
    }

    @Override
    public CompletableFuture<AppendResponse> append(AppendRequest request) {
        executionContext.checkThread();

        // If the request indicates a term that is greater than the current term then
        // assign that term and leader to the current context and step down as a candidate.
        if (request.term() >= context.getTerm()) {
            context.setTerm(request.term());
            transition(RaftStateType.FOLLOWER);
        }
        return super.append(request);
    }

    @Override
    public CompletableFuture<VoteResponse> vote(VoteRequest request) {
        executionContext.checkThread();

        // If the request indicates a term that is greater than the current term then
        // assign that term and leader to the current context and step down as a candidate.
        if (request.term() > context.getTerm()) {
            context.setTerm(request.term());
            transition(RaftStateType.FOLLOWER);
            return super.vote(request);
        }

        // If the vote request is not for this candidate then reject the vote.
        if (request.candidate().equals(transportService.localNode())) {
            return Futures.complete(VoteResponse.builder()
                .setTerm(context.getTerm())
                .setVoted(true)
                .build());
        } else {
            return Futures.complete(VoteResponse.builder()
                .setTerm(context.getTerm())
                .setVoted(false)
                .build());
        }
    }

    private void cancelElection() {
        executionContext.checkThread();
        if (currentTimer != null) {
            logger.info("cancelling election");
            currentTimer.cancel(false);
        }
        if (quorum != null) {
            quorum.cancel();
            quorum = null;
        }
    }

    public synchronized void close() {
        executionContext.checkThread();
        cancelElection();
    }
}
