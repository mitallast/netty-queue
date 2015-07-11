package org.mitallast.queue.raft.state;

import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.unit.TimeValue;
import org.mitallast.queue.raft.Command;
import org.mitallast.queue.raft.ConsistencyLevel;
import org.mitallast.queue.raft.NoLeaderException;
import org.mitallast.queue.raft.Query;
import org.mitallast.queue.raft.action.command.CommandRequest;
import org.mitallast.queue.raft.action.command.CommandResponse;
import org.mitallast.queue.raft.action.keepalive.KeepAliveRequest;
import org.mitallast.queue.raft.action.keepalive.KeepAliveResponse;
import org.mitallast.queue.raft.action.query.QueryRequest;
import org.mitallast.queue.raft.action.query.QueryResponse;
import org.mitallast.queue.raft.action.register.RegisterRequest;
import org.mitallast.queue.raft.action.register.RegisterResponse;
import org.mitallast.queue.raft.cluster.Member;
import org.mitallast.queue.raft.cluster.TransportCluster;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RaftStateClient extends AbstractLifecycleComponent {
    private final TransportCluster transportCluster;
    private final ExecutionContext executionContext;
    private final AtomicBoolean keepAlive = new AtomicBoolean();
    private final Random random = new Random();
    private final long keepAliveInterval;

    protected volatile DiscoveryNode leader;
    protected volatile long term;
    protected volatile long session;

    private volatile long request;
    private volatile long response;
    private volatile long version;

    private volatile ScheduledFuture<?> currentTimer;
    private volatile ScheduledFuture<?> registerTimer;

    @Inject
    public RaftStateClient(Settings settings, TransportCluster transportCluster, ExecutionContext executionContext) {
        super(settings);
        this.transportCluster = transportCluster;
        this.executionContext = executionContext;
        this.keepAliveInterval = componentSettings.getAsTime("keep_alive", TimeValue.timeValueSeconds(1)).millis();
    }

    public DiscoveryNode getLeader() {
        executionContext.checkThread();
        return leader;
    }

    RaftStateClient setLeader(DiscoveryNode leader) {
        executionContext.checkThread();
        this.leader = leader;
        return this;
    }

    public long getTerm() {
        executionContext.checkThread();
        return term;
    }

    RaftStateClient setTerm(long term) {
        executionContext.checkThread();
        this.term = term;
        return this;
    }

    public long getSession() {
        executionContext.checkThread();
        return session;
    }

    RaftStateClient setSession(long session) {
        executionContext.checkThread();
        this.session = session;
        this.request = 0;
        this.response = 0;
        this.version = 0;
        return this;
    }

    public long getRequest() {
        executionContext.checkThread();
        return request;
    }

    RaftStateClient setRequest(long request) {
        executionContext.checkThread();
        this.request = request;
        return this;
    }

    public long getResponse() {
        executionContext.checkThread();
        return response;
    }

    RaftStateClient setResponse(long response) {
        executionContext.checkThread();
        this.response = response;
        return this;
    }

    public long getVersion() {
        executionContext.checkThread();
        return version;
    }

    RaftStateClient setVersion(long version) {
        executionContext.checkThread();
        if (version > this.version)
            this.version = version;
        return this;
    }

    @SuppressWarnings("unchecked")
    public <R extends Streamable> CompletableFuture<R> submit(Command<R> command) {
        CompletableFuture<R> future = Futures.future();
        executionContext.execute(() -> {
            if (session == 0)
                future.completeExceptionally(new IllegalStateException("session not open"));

            Member member;
            try {
                member = selectMember(command);
            } catch (IllegalStateException e) {
                future.completeExceptionally(e);
                return;
            }

            if (member == null) {
                setLeader(null);
                future.completeExceptionally(new IllegalStateException("unknown leader"));
            } else {
                // TODO: This should retry on timeouts with the same request ID.
                long requestId = ++request;
                CommandRequest request = CommandRequest.builder()
                    .setSession(getSession())
                    .setRequest(requestId)
                    .setResponse(getResponse())
                    .setCommand(command)
                    .build();

                member.<CommandRequest, CommandResponse>send(request).whenCompleteAsync((response, error) -> {
                    if (error == null) {
                        future.complete((R) response.result());
                        setResponse(Math.max(getResponse(), requestId));
                    } else {
                        future.completeExceptionally(error);
                    }
                }, executionContext.executor());
            }
        });
        return future;
    }

    /**
     * Selects the member to which to send the given command.
     */
    protected Member selectMember(Command<?> command) {
        executionContext.checkThread();
        if (leader == null)
            throw new IllegalStateException("unknown leader");
        return transportCluster.member(leader);
    }

    /**
     * Submits a query.
     *
     * @param query The query to submit.
     * @param <R>   The query result type.
     * @return A completable future to be completed with the query result.
     */
    @SuppressWarnings("unchecked")
    public <R extends Streamable> CompletableFuture<R> submit(Query<R> query) {
        CompletableFuture<R> future = Futures.future();
        executionContext.execute(() -> {
            if (leader == null)
                future.completeExceptionally(new IllegalStateException("unknown leader"));
            if (session == 0)
                future.completeExceptionally(new IllegalStateException("session not open"));

            Member member;
            try {
                member = selectMember(query);
            } catch (IllegalStateException e) {
                future.completeExceptionally(e);
                return;
            }

            if (member == null) {
                setLeader(null);
                future.completeExceptionally(new IllegalStateException("unknown leader"));
            } else {
                QueryRequest request = QueryRequest.builder()
                    .setSession(getSession())
                    .setQuery(query)
                    .build();
                member.<QueryRequest, QueryResponse>send(request).whenCompleteAsync((response, error) -> {
                    if (error == null) {
                        future.complete((R) response.result());
                    } else {
                        future.completeExceptionally(error);
                    }
                }, executionContext.executor());
            }
        });
        return future;
    }

    protected Member selectMember(Query<?> query) {
        executionContext.checkThread();
        ConsistencyLevel level = query.consistency();
        if (level.isLeaderRequired()) {
            return transportCluster.member(getLeader());
        } else {
            return transportCluster.members().get(random.nextInt(transportCluster.members().size()));
        }
    }

    private CompletableFuture<Void> register() {
        executionContext.checkThread();
        return register(100, Futures.future());
    }

    private CompletableFuture<Void> register(long interval, CompletableFuture<Void> future) {
        executionContext.checkThread();
        register(new ArrayList<>(transportCluster.members())).whenComplete((result, error) -> {
            if (error == null) {
                future.complete(null);
            } else {
                long nextInterval = Math.min(interval * 2, 5000);
                registerTimer = executionContext.schedule(() -> register(nextInterval, future), nextInterval, TimeUnit.MILLISECONDS);
            }
        });
        return future;
    }

    protected CompletableFuture<Void> register(List<Member> members) {
        executionContext.checkThread();
        return register(members, Futures.future()).thenCompose(response -> {
            setTerm(response.term());
            setLeader(response.leader());
            setSession(response.session());
            return this.transportCluster.configure(response.members());
        });
    }

    /**
     * Registers the client by contacting a random member.
     */
    protected CompletableFuture<RegisterResponse> register(List<Member> members, CompletableFuture<RegisterResponse> future) {
        executionContext.checkThread();
        if (members.isEmpty()) {
            future.completeExceptionally(new NoLeaderException("no leader found"));
            return future;
        }

        Member member = selectMember(members);

        logger.info("registering session via {}", member.node());
        RegisterRequest request = RegisterRequest.builder()
            .setMember(member.node())
            .build();
        member.<RegisterRequest, RegisterResponse>send(request).whenCompleteAsync((response, error) -> {
            if (error == null) {
                future.complete(response);
                logger.info("registered new session: {}", getSession());
            } else {
                logger.warn("session registration failed, retrying {}", error.getMessage());
                logger.debug("session registration failed error", error);
                setLeader(null);
                register(members, future);
            }
        }, executionContext.executor());
        return future;
    }

    /**
     * Starts the keep alive timer.
     */
    private void startKeepAliveTimer() {
        executionContext.checkThread();
        logger.info("starting keep alive timer");
        currentTimer = executionContext.scheduleAtFixedRate(this::keepAlive, 1, keepAliveInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * Sends a keep alive request to a random member.
     */
    private void keepAlive() {
        executionContext.checkThread();
        if (keepAlive.compareAndSet(false, true)) {
            logger.debug("sending keep alive request");
            keepAlive(transportCluster.members().stream()
                .filter(m -> m.type() == Member.Type.ACTIVE)
                .collect(Collectors.toList())).thenRun(() -> keepAlive.set(false));
        }
    }

    /**
     * Sends a keep alive request.
     */
    protected CompletableFuture<Void> keepAlive(List<Member> members) {
        executionContext.checkThread();
        return keepAlive(members, Futures.future()).thenCompose(response -> {
            setTerm(response.term());
            setLeader(response.leader());
            setVersion(response.version());
            return this.transportCluster.configure(response.members());
        });
    }

    /**
     * Registers the client by contacting a random member.
     */
    protected CompletableFuture<KeepAliveResponse> keepAlive(List<Member> members, CompletableFuture<KeepAliveResponse> future) {
        executionContext.checkThread();
        if (members.isEmpty()) {
            future.completeExceptionally(new NoLeaderException());
            keepAlive.set(false);
            return future;
        }

        Member member = selectMember(members);

        KeepAliveRequest request = KeepAliveRequest.builder()
            .setSession(getSession())
            .build();
        member.<KeepAliveRequest, KeepAliveResponse>send(request).whenCompleteAsync((response, error) -> {
            if (error == null) {
                future.complete(response);
            } else {
                keepAlive(members, future);
            }
        }, executionContext.executor());
        return future;
    }

    /**
     * Selects a random member from the given members list.
     */
    protected Member selectMember(List<Member> members) {
        executionContext.checkThread();
        if (leader != null) {
            for (int i = 0; i < members.size(); i++) {
                if (leader.address().equals(members.get(i).node().address())) {
                    return members.remove(i);
                }
            }
            setLeader(null);
            return members.remove(random.nextInt(members.size()));
        } else {
            return members.remove(random.nextInt(members.size()));
        }
    }

    private void cancelRegisterTimer() {
        executionContext.checkThread();
        if (registerTimer != null) {
            logger.info("cancelling register timer");
            registerTimer.cancel(false);
        }
    }

    private void cancelKeepAliveTimer() {
        executionContext.checkThread();
        if (currentTimer != null) {
            logger.info("cancelling keep alive timer");
            currentTimer.cancel(false);
        }
    }

    @Override
    protected void doStart() throws IOException {
        try {
            executionContext.submit(() -> register().thenRun(this::startKeepAliveTimer)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e.getCause());
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    protected void doStop() throws IOException {
        try {
            executionContext.submit(() -> {
                cancelRegisterTimer();
                cancelKeepAliveTimer();
            }).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void doClose() throws IOException {

    }
}
