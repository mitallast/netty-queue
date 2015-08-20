package org.mitallast.queue.raft.state;

import com.google.common.collect.ImmutableList;
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
import org.mitallast.queue.raft.cluster.ClusterService;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class RaftStateClient extends AbstractLifecycleComponent {
    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final ExecutionContext executionContext;
    private final AtomicBoolean keepAlive = new AtomicBoolean();
    private final Random random = new Random();
    private final long keepAliveInterval;
    private final long registerDelay;

    protected volatile DiscoveryNode leader;
    protected volatile long term;
    protected volatile long session;

    private volatile long request;
    private volatile long response;
    private volatile long version;

    private volatile ScheduledFuture<?> keepAliveTimer;
    private volatile ScheduledFuture<?> registerTimer;

    public RaftStateClient(Settings settings, TransportService transportService, ClusterService clusterService, ExecutionContext executionContext) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.executionContext = executionContext;
        this.keepAliveInterval = componentSettings.getAsTime("keep_alive", TimeValue.timeValueMinutes(3)).millis();
        this.registerDelay = componentSettings.getAsTime("register_delay", TimeValue.timeValueMillis(100)).millis();
    }

    public DiscoveryNode getLeader() {
        executionContext.checkThread();
        return leader;
    }

    RaftStateClient setLeader(DiscoveryNode leader) {
        executionContext.checkThread();
        logger.info("new leader {}", leader);
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

            DiscoveryNode member;
            try {
                member = selectLeader();
            } catch (IllegalStateException e) {
                future.completeExceptionally(e);
                return;
            }

            if (member == null) {
                logger.info("no member found, set no leader");
                setLeader(null);
                future.completeExceptionally(new IllegalStateException("unknown leader"));
            } else {
                CommandRequest request = CommandRequest.builder()
                    .setSession(getSession())
                    .setRequest(++this.request)
                    .setResponse(getResponse())
                    .setCommand(command)
                    .build();

                submit(request, future);
            }
        });
        return future;
    }

    @SuppressWarnings("unchecked")
    private <R extends Streamable> void submit(CommandRequest request, CompletableFuture<R> future) {
        DiscoveryNode member;
        try {
            member = selectLeader();
        } catch (IllegalStateException e) {
            future.completeExceptionally(e);
            return;
        }
        if (member == null) {
            logger.info("no member found, set no leader");
            setLeader(null);
            future.completeExceptionally(new IllegalStateException("unknown leader"));
        } else {
            CompletableFuture<CommandResponse> transportFuture = transportService.client(member.address()).<CommandRequest, CommandResponse>send(request);
            // retry
            ScheduledFuture<?> scheduledFuture = executionContext.schedule(() -> {
                transportFuture.cancel(false);
                if (!future.isCancelled()) {
                    submit(request, future);
                }
            }, 1, TimeUnit.MINUTES);
            // response handler
            transportFuture.whenCompleteAsync((response, error) -> {
                scheduledFuture.cancel(false);
                if (error == null) {
                    future.complete((R) response.result());
                    setResponse(Math.max(getResponse(), request.request()));
                } else {
                    future.completeExceptionally(error);
                }
            }, executionContext.executor());
        }
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

            DiscoveryNode member;
            try {
                member = selectMember(query);
            } catch (IllegalStateException e) {
                future.completeExceptionally(e);
                return;
            }

            if (member == null) {
                logger.info("no member found, set no leader");
                setLeader(null);
                future.completeExceptionally(new IllegalStateException("unknown leader"));
            } else {
                QueryRequest request = QueryRequest.builder()
                    .setSession(getSession())
                    .setQuery(query)
                    .build();
                transportService.client(member.address()).<QueryRequest, QueryResponse>send(request).whenCompleteAsync((response, error) -> {
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

    /**
     * Selects leader to which to send the given command.
     *
     * @throws IllegalStateException if no leader found
     */
    protected DiscoveryNode selectLeader() {
        executionContext.checkThread();
        if (leader == null)
            throw new IllegalStateException("unknown leader");
        return leader;
    }

    /**
     * Selects the node to which to send the given command.
     *
     * @throws IllegalStateException if no leader found
     */
    protected DiscoveryNode selectMember(Query<?> query) {
        executionContext.checkThread();
        ConsistencyLevel level = query.consistency();
        if (level.isLeaderRequired()) {
            return selectLeader();
        } else {
            ImmutableList<DiscoveryNode> nodes = clusterService.nodes();
            return nodes.get(random.nextInt(nodes.size()));
        }
    }

    private CompletableFuture<Void> register() {
        CompletableFuture<Void> future = Futures.future();
        executionContext.execute(() -> register(future));
        return future;
    }

    private void register(CompletableFuture<Void> future) {
        logger.info("register start");
        executionContext.checkThread();
        register(new ArrayList<>(clusterService.nodes())).whenComplete((result, error) -> {
            if (error == null) {
                logger.info("register success");
                future.complete(null);
            } else {
                logger.error("register error, schedule next try: {}", error.getMessage());
                registerTimer = executionContext.schedule(() -> register(future), registerDelay, TimeUnit.MILLISECONDS);
            }
        });
    }

    protected CompletableFuture<Void> register(List<DiscoveryNode> members) {
        executionContext.checkThread();
        CompletableFuture<RegisterResponse> future = Futures.future();
        register(members, future);
        return future.thenCompose(response -> {
            logger.info("register send success, compose");
            setTerm(response.term());
            setLeader(response.leader());
            setSession(response.session());
            return Futures.complete(null);
        });
    }

    /**
     * Registers the client by contacting a random member.
     */
    protected void register(List<DiscoveryNode> members, CompletableFuture<RegisterResponse> future) {
        executionContext.checkThread();
        if (members.isEmpty()) {
            logger.error("register send error, not leader found");
            future.completeExceptionally(new NoLeaderException("no leader found"));
            return;
        }

        DiscoveryNode member = selectMember(members);

        logger.info("register send to {}", member);
        RegisterRequest request = RegisterRequest.builder()
            .setMember(transportService.localNode())
            .build();

        transportService.connectToNode(member.address());
        transportService.client(member.address()).<RegisterRequest, RegisterResponse>send(request).whenCompleteAsync((response, error) -> {
            if (error == null) {
                logger.info("register send success,  new session: {}", getSession());
                future.complete(response);
            } else {
                logger.warn("register send error, retrying: {}", error.getMessage());
                register(members, future);
            }
        }, executionContext.executor());
    }

    /**
     * Starts the keep alive timer.
     */
    private void startKeepAliveTimer() {
        logger.info("starting keep alive timer");
        keepAliveTimer = executionContext.scheduleAtFixedRate(this::keepAlive, 1, keepAliveInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * Sends a keep alive request to a random member.
     */
    private void keepAlive() {
        executionContext.checkThread();
        if (keepAlive.compareAndSet(false, true)) {
            logger.debug("sending keep alive request");
            keepAlive(new ArrayList<>(clusterService.nodes())).thenRun(() -> keepAlive.set(false));
        }
    }

    /**
     * Sends a keep alive request.
     */
    protected CompletableFuture<Void> keepAlive(List<DiscoveryNode> members) {
        executionContext.checkThread();
        return keepAlive(members, Futures.future()).thenCompose(response -> {
            setTerm(response.term());
            setLeader(response.leader());
            setVersion(response.version());
            return Futures.complete(null);
        });
    }

    /**
     * Registers the client by contacting a random member.
     */
    protected CompletableFuture<KeepAliveResponse> keepAlive(List<DiscoveryNode> members, CompletableFuture<KeepAliveResponse> future) {
        executionContext.checkThread();
        if (members.isEmpty()) {
            future.completeExceptionally(new NoLeaderException());
            keepAlive.set(false);
            return future;
        }

        DiscoveryNode member = selectMember(members);

        KeepAliveRequest request = KeepAliveRequest.builder()
            .setSession(getSession())
            .build();

        transportService.client(member.address()).<KeepAliveRequest, KeepAliveResponse>send(request).whenCompleteAsync((response, error) -> {
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
    protected DiscoveryNode selectMember(List<DiscoveryNode> members) {
        executionContext.checkThread();
        if (leader != null) {
            if (leader.equals(transportService.localNode())) {
                logger.info("local node is leader, ignore members");
                return leader;
            }
            for (int i = 0; i < members.size(); i++) {
                if (leader.equals(members.get(i))) {
                    return members.remove(i);
                }
            }
            logger.info("set no leader on selecting member");
            setLeader(null);
            return members.remove(random.nextInt(members.size()));
        } else {
            return members.remove(random.nextInt(members.size()));
        }
    }

    @Override
    protected void doStart() throws IOException {
        try {
            register().get();
            startKeepAliveTimer();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void doStop() throws IOException {
        if (registerTimer != null) {
            logger.info("cancelling register timer");
            registerTimer.cancel(false);
        }
        if (keepAliveTimer != null) {
            logger.info("cancelling keep alive timer");
            keepAliveTimer.cancel(false);
        }
    }

    @Override
    protected void doClose() throws IOException {

    }
}
