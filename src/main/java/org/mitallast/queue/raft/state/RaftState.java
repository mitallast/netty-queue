package org.mitallast.queue.raft.state;

import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.unit.TimeValue;
import org.mitallast.queue.raft.*;
import org.mitallast.queue.raft.log.compaction.Compaction;
import org.mitallast.queue.raft.log.entry.*;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class RaftState extends AbstractComponent implements EntryFilter {
    private final StateMachine stateMachine;
    private final ClusterService members;
    private final ExecutionContext executionContext;
    private final Map<Long, RaftSession> sessions = new ConcurrentHashMap<>();
    private final Map<Long, List<Runnable>> queries = new ConcurrentHashMap<>();
    private final long sessionTimeout;

    private volatile long lastApplied;

    @Inject
    public RaftState(Settings settings, StateMachine stateMachine, ClusterService members, ExecutionContext executionContext) {
        super(settings);
        this.stateMachine = stateMachine;
        this.members = members;
        this.executionContext = executionContext;
        this.sessionTimeout = componentSettings.getAsTime("session_timeout", TimeValue.timeValueMinutes(5)).millis();
    }

    public long getLastApplied() {
        executionContext.checkThread();
        return lastApplied;
    }

    private void setLastApplied(long lastApplied) {
        logger.trace("last applied {}", lastApplied);
        executionContext.checkThread();
        this.lastApplied = lastApplied;
        List<Runnable> queries = this.queries.remove(lastApplied);
        if (queries != null) {
            queries.forEach(Runnable::run);
        }
    }

    public boolean filter(RaftLogEntry entry, Compaction compaction) {
        executionContext.checkThread();
        if (entry instanceof CommandEntry) {
            return filter((CommandEntry) entry, compaction);
        } else if (entry instanceof KeepAliveEntry) {
            return filter((KeepAliveEntry) entry, compaction);
        } else if (entry instanceof RegisterEntry) {
            return filter((RegisterEntry) entry, compaction);
        } else if (entry instanceof NoOpEntry) {
            return filter((NoOpEntry) entry, compaction);
        } else if (entry instanceof JoinEntry) {
            return filter((JoinEntry) entry, compaction);
        } else if (entry instanceof LeaveEntry) {
            return filter((LeaveEntry) entry, compaction);
        }
        return false;
    }

    public boolean filter(RegisterEntry entry, Compaction compaction) {
        executionContext.checkThread();
        return sessions.containsKey(entry.index());
    }

    public boolean filter(KeepAliveEntry entry, Compaction compaction) {
        executionContext.checkThread();
        return sessions.containsKey(entry.index()) && sessions.get(entry.index()).index == entry.index();
    }

    public boolean filter(NoOpEntry entry, Compaction compaction) {
        executionContext.checkThread();
        return false;
    }

    public boolean filter(CommandEntry entry, Compaction compaction) {
        executionContext.checkThread();
        RaftSession session = sessions.get(entry.session());
        if (session == null) {
            session = new RaftSession(entry.session(), entry.timestamp());
            session.expire();
        }
        Commit<? extends Command> commit = new Commit<>(entry.index(), session, entry.timestamp(), entry.command());
        return stateMachine.filter(commit, compaction);
    }

    public boolean filter(JoinEntry entry, Compaction compaction) {
        executionContext.checkThread();
        MemberState member = members.getMember(entry.getMember());
        return member != null && member.getVersion() == entry.index();
    }

    public boolean filter(LeaveEntry entry, Compaction compaction) {
        executionContext.checkThread();
        return !compaction.type().isOrdered();
    }

    public CompletableFuture<?> apply(RaftLogEntry entry) {
        executionContext.checkThread();
        if (entry instanceof CommandEntry) {
            return apply((CommandEntry) entry);
        } else if (entry instanceof QueryEntry) {
            return apply((QueryEntry) entry);
        } else if (entry instanceof RegisterEntry) {
            return apply((RegisterEntry) entry);
        } else if (entry instanceof KeepAliveEntry) {
            return apply((KeepAliveEntry) entry);
        } else if (entry instanceof NoOpEntry) {
            return apply((NoOpEntry) entry);
        } else if (entry instanceof JoinEntry) {
            return apply((JoinEntry) entry);
        } else if (entry instanceof LeaveEntry) {
            return apply((LeaveEntry) entry);
        }
        return Futures.completeExceptionally(new InternalException("unknown state machine operation"));
    }

    public CompletableFuture<Long> apply(RegisterEntry entry) {
        executionContext.checkThread();
        return register(entry.index(), entry.timestamp());
    }

    public CompletableFuture<Void> apply(KeepAliveEntry entry) {
        executionContext.checkThread();
        return keepAlive(entry.index(), entry.timestamp(), entry.session());
    }

    public CompletableFuture<Streamable> apply(CommandEntry entry) {
        executionContext.checkThread();
        return command(entry.index(), entry.session(), entry.request(), entry.response(), entry.timestamp(), entry.command());
    }

    public CompletableFuture<Streamable> apply(QueryEntry entry) {
        executionContext.checkThread();
        return query(entry.index(), entry.session(), entry.version(), entry.timestamp(), entry.query());
    }

    public CompletableFuture<Long> apply(NoOpEntry entry) {
        executionContext.checkThread();
        return noop(entry.index());
    }

    public CompletableFuture<Long> apply(JoinEntry entry) {
        executionContext.checkThread();
        return join(entry.index(), entry.getMember());
    }

    public CompletableFuture<Void> apply(LeaveEntry entry) {
        executionContext.checkThread();
        return leave(entry.index(), entry.getMember());
    }

    private CompletableFuture<Long> register(long index, long timestamp) {
        executionContext.checkThread();
        logger.info("new session {}", index);
        RaftSession session = new RaftSession(index, timestamp);
        sessions.put(index, session);
        setLastApplied(index);
        return Futures.complete(session.id());
    }

    private CompletableFuture<Void> keepAlive(long index, long timestamp, long sessionId) {
        executionContext.checkThread();
        RaftSession session = sessions.get(sessionId);

        // We need to ensure that the command is applied to the state machine before queries are run.
        // Set last applied only after the operation has been submitted to the state machine executor.
        CompletableFuture<Void> future;
        if (session == null) {
            logger.warn("session {} not found, available: {}", sessionId, sessions.keySet());
            future = Futures.completeExceptionally(new UnknownSessionException("unknown session: " + sessionId));
        } else if (!session.update(index, timestamp)) {
            sessions.remove(sessionId);
            future = Futures.completeExceptionally(new UnknownSessionException("session expired: " + sessionId));
        } else {
            future = Futures.complete(null);
        }

        setLastApplied(index);
        return future;
    }

    private CompletableFuture<Long> noop(long index) {
        executionContext.checkThread();
        // We need to ensure that the command is applied to the state machine before queries are run.
        // Set last applied only after the operation has been submitted to the state machine executor.
        CompletableFuture<Long> future = CompletableFuture.supplyAsync(() -> index, executionContext.executor());
        setLastApplied(index);
        return future;
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<Streamable> command(long index, long sessionId, long request, long response, long timestamp, Command command) {
        executionContext.checkThread();
        CompletableFuture<Streamable> future;

        // First check to ensure that the session exists.
        RaftSession session = sessions.get(sessionId);
        if (session == null) {
            future = Futures.completeExceptionally(new UnknownSessionException("unknown session " + sessionId));
        } else if (!session.update(index, timestamp)) {
            sessions.remove(sessionId);
            future = Futures.completeExceptionally(new UnknownSessionException("unknown session " + sessionId));
        } else if (session.responses.containsKey(request)) {
            future = Futures.complete(session.responses.get(request));
        } else {
            // Apply the command to the state machine.
            future = CompletableFuture.supplyAsync(() -> stateMachine.apply(new Commit(index, session, timestamp, command)), executionContext.executor());
            future.thenApply((Streamable result) -> {
                // Store the command result in the session.
                session.put(request, result);

                // Clear any responses that have been received by the client for the session.
                session.responses.headMap(response, true).clear();
                return result;
            });
        }

        // We need to ensure that the command is applied to the state machine before queries are run.
        // Set last applied only after the operation has been submitted to the state machine executor.
        setLastApplied(index);
        return future;
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<Streamable> query(long index, long sessionId, long version, long timestamp, Query query) {
        executionContext.checkThread();
        // If the session has not yet been opened or if the client provided a version greater than the last applied index
        // then wait until the up-to-date index is applied to the state machine.
        if (sessionId > lastApplied || version > lastApplied) {
            CompletableFuture<Streamable> future = Futures.future();
            List<Runnable> queries = this.queries.computeIfAbsent(Math.max(sessionId, version), id -> new ArrayList<>());
            queries.add(() -> {
                CompletableFuture<Streamable> applyFuture = CompletableFuture.supplyAsync(() ->
                    stateMachine.apply(new Commit(index, sessions.get(sessionId), timestamp, query)), executionContext.executor());
                applyFuture.whenComplete((result, error) -> {
                    if (error == null) {
                        future.complete(result);
                    } else {
                        future.completeExceptionally(error);
                    }
                });
            });
            return future;
        } else {
            // Verify that the client's session is still alive.
            RaftSession session = sessions.get(sessionId);
            if (session == null) {
                return Futures.completeExceptionally(new UnknownSessionException("unknown session: " + sessionId));
            } else if (!session.expire(timestamp)) {
                return Futures.completeExceptionally(new UnknownSessionException("unknown session: " + sessionId));
            } else {
                return CompletableFuture.supplyAsync(() -> stateMachine.apply(new Commit(index, session, timestamp, query)), executionContext.executor());
            }
        }
    }

    private CompletableFuture<Long> join(long index, DiscoveryNode node) {
        executionContext.checkThread();
        logger.info("join node index: {} node: {}", index, node);
        MemberState member = members.getMember(node);
        if (member == null) {
            member = new MemberState(node, MemberState.Type.PASSIVE).setVersion(index);
            members.addMember(member);
        }
        setLastApplied(index);
        return Futures.complete(index);
    }

    private CompletableFuture<Void> leave(long index, DiscoveryNode node) {
        executionContext.checkThread();
        MemberState member = members.getMember(node);
        if (member != null) {
            members.removeMember(member);
        }
        setLastApplied(index);
        return Futures.complete(null);
    }

    @Override
    public boolean accept(RaftLogEntry entry, Compaction compaction) {
        executionContext.checkThread();
        return this.filter(entry, compaction);
    }

    private class RaftSession extends Session {
        private final TreeMap<Long, Streamable> responses = new TreeMap<>();
        private long index;
        private long timestamp;

        private RaftSession(long id, long timestamp) {
            super(id);
            executionContext.checkThread();
            this.timestamp = timestamp;
        }

        public void expire() {
            executionContext.checkThread();
            super.expire();
        }

        public long timestamp() {
            executionContext.checkThread();
            return timestamp;
        }

        private boolean expire(long timestamp) {
            executionContext.checkThread();
            if (timestamp - sessionTimeout > this.timestamp) {
                expire();
                return false;
            }
            this.timestamp = timestamp;
            return true;
        }

        private boolean update(long index, long timestamp) {
            executionContext.checkThread();
            if (timestamp - sessionTimeout > this.timestamp) {
                expire();
                return false;
            }
            this.index = index;
            this.timestamp = timestamp;
            return true;
        }

        private void put(long request, Streamable response) {
            executionContext.checkThread();
            responses.put(request, response);
        }
    }
}
