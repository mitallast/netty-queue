package org.mitallast.queue.raft.resource.structures;


import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.*;
import org.mitallast.queue.raft.log.Compaction;
import org.mitallast.queue.raft.resource.AbstractResource;
import org.mitallast.queue.raft.resource.Mode;
import org.mitallast.queue.raft.resource.Stateful;
import org.mitallast.queue.raft.resource.result.BooleanResult;
import org.mitallast.queue.raft.resource.result.IntegerResult;
import org.mitallast.queue.raft.resource.result.VoidResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Stateful(AsyncMap.StateMachine.class)
@SuppressWarnings("unused")
public class AsyncMap<K extends Streamable, V extends Streamable> extends AbstractResource {

    public AsyncMap(Protocol protocol) {
        super(protocol);
    }

    public CompletableFuture<Boolean> isEmpty() {
        return submit(IsEmpty.<K, V>builder().build())
            .thenApply(BooleanResult::get);
    }

    public CompletableFuture<Boolean> isEmpty(ConsistencyLevel consistency) {
        return submit(IsEmpty.<K, V>builder()
            .setConsistency(consistency)
            .build())
            .thenApply(BooleanResult::get);
    }

    public CompletableFuture<Integer> size() {
        return submit(Size.<K, V>builder().build())
            .thenApply(IntegerResult::get);
    }

    public CompletableFuture<Integer> size(ConsistencyLevel consistency) {
        return submit(Size.<K, V>builder()
            .setConsistency(consistency)
            .build())
            .thenApply(IntegerResult::get);
    }

    public CompletableFuture<Boolean> containsKey(K key) {
        return submit(ContainsKey.<K>builder()
            .setKey(key)
            .build())
            .thenApply(BooleanResult::get);
    }

    public CompletableFuture<Boolean> containsKey(K key, ConsistencyLevel consistency) {
        return submit(ContainsKey.<K>builder()
            .setKey(key)
            .setConsistency(consistency)
            .build())
            .thenApply(BooleanResult::get);
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<V> get(K key) {
        return submit(Get.<K, V>builder()
            .setKey(key)
            .build());
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<V> get(K key, ConsistencyLevel consistency) {
        return submit(Get.<K, V>builder()
            .setKey(key)
            .setConsistency(consistency)
            .build());
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<V> put(K key, V value) {
        return submit(Put.<K, V>builder()
            .setKey(key)
            .setValue(value)
            .build());
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<V> put(K key, V value, Mode mode) {
        return submit(Put.<K, V>builder()
            .setKey(key)
            .setValue(value)
            .setMode(mode)
            .build());
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<V> put(K key, V value, long ttl) {
        return submit(Put.<K, V>builder()
            .setKey(key)
            .setValue(value)
            .setTtl(ttl)
            .build());
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<V> put(K key, V value, long ttl, Mode mode) {
        return submit(Put.<K, V>builder()
            .setKey(key)
            .setValue(value)
            .setTtl(ttl)
            .setMode(mode)
            .build());
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<V> remove(K key) {
        return submit(Remove.<K, V>builder()
            .setKey(key)
            .build());
    }

    public CompletableFuture<V> remove(K key, V value) {
        return submit(Remove.<K, V>builder()
            .setKey(key)
            .setValue(value)
            .build());
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<V> getOrDefault(K key, V defaultValue) {
        return submit(GetOrDefault.<K, V>builder()
            .setKey(key)
            .setDefaultValue(defaultValue)
            .build());
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<V> getOrDefault(K key, V defaultValue, ConsistencyLevel consistency) {
        return submit(GetOrDefault.<K, V>builder()
            .setKey(key)
            .setDefaultValue(defaultValue)
            .setConsistency(consistency)
            .build());
    }

    public CompletableFuture<V> putIfAbsent(K key, V value) {
        return submit(PutIfAbsent.<K, V>builder()
            .setKey(key)
            .setValue(value)
            .build());
    }

    public CompletableFuture<V> putIfAbsent(K key, V value, long ttl) {
        return submit(PutIfAbsent.<K, V>builder()
            .setKey(key)
            .setValue(value)
            .setTtl(ttl)
            .build());
    }

    public CompletableFuture<V> putIfAbsent(K key, V value, long ttl, Mode mode) {
        return submit(PutIfAbsent.<K, V>builder()
            .setKey(key)
            .setValue(value)
            .setTtl(ttl)
            .setMode(mode)
            .build());
    }

    public CompletableFuture<V> putIfAbsent(K key, V value, long ttl, TimeUnit unit) {
        return submit(PutIfAbsent.<K, V>builder()
            .setKey(key)
            .setValue(value)
            .setTtl(ttl)
            .build());
    }

    public CompletableFuture<V> putIfAbsent(K key, V value, long ttl, TimeUnit unit, Mode mode) {
        return submit(PutIfAbsent.<K, V>builder()
            .setKey(key)
            .setValue(value)
            .setTtl(ttl)
            .setMode(mode)
            .build());
    }

    public CompletableFuture<Void> clear() {
        return submit(Clear.<K, V>builder().build()).thenApply(voidResult -> null);
    }

    public static abstract class MapCommand<V extends Streamable> implements Command<V> {
    }

    public static abstract class MapQuery<V extends Streamable> implements Query<V> {
        protected final ConsistencyLevel consistency;

        public MapQuery(ConsistencyLevel consistency) {
            this.consistency = consistency;
        }

        @Override
        public ConsistencyLevel consistency() {
            return consistency;
        }

        public static abstract class Builder<B extends Builder<B, E, V>, E extends MapQuery<V>, V extends Streamable> implements EntryBuilder<E> {
            protected ConsistencyLevel consistency = ConsistencyLevel.LINEARIZABLE_LEASE;

            @SuppressWarnings("unchecked")
            public B from(E entry) {
                consistency = entry.consistency;
                return (B) this;
            }

            public Builder<B, E, V> setConsistency(ConsistencyLevel consistency) {
                this.consistency = consistency;
                return this;
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
                consistency = stream.readEnum(ConsistencyLevel.class);
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
                stream.writeEnum(consistency);
            }
        }
    }

    public static abstract class KeyCommand<K extends Streamable, V extends Streamable> extends MapCommand<V> {
        protected final K key;

        public KeyCommand(K key) {
            this.key = key;
        }

        public K key() {
            return key;
        }

        public static abstract class Builder<B extends Builder<B, E, K, V>, E extends KeyCommand<K, V>, K extends Streamable, V extends Streamable>
            implements EntryBuilder<E> {
            protected K key;

            @SuppressWarnings("unchecked")
            public B from(E entry) {
                key = entry.key;
                return (B) this;
            }

            @SuppressWarnings("unchecked")
            public B setKey(K key) {
                this.key = key;
                return (B) this;
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
                key = stream.readStreamable();
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
                stream.writeClass(key.getClass());
                stream.writeStreamable(key);
            }
        }
    }

    public static abstract class KeyQuery<K extends Streamable, V extends Streamable> extends MapQuery<V> {
        protected final K key;

        public KeyQuery(ConsistencyLevel consistency, K key) {
            super(consistency);
            this.key = key;
        }

        public K key() {
            return key;
        }

        public static abstract class Builder<B extends Builder<B, E, K, T>, E extends KeyQuery<K, T>, K extends Streamable, T extends Streamable>
            extends MapQuery.Builder<B, E, T> {
            protected K key;

            @Override
            public B from(E entry) {
                key = entry.key;
                return super.from(entry);
            }

            @SuppressWarnings("unchecked")
            public B setKey(K key) {
                this.key = key;
                return (B) this;
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
                super.readFrom(stream);
                key = stream.readStreamable();
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
                super.writeTo(stream);
                stream.writeClass(key.getClass());
                stream.writeStreamable(key);
            }
        }
    }

    public static abstract class KeyValueCommand<K extends Streamable, V extends Streamable> extends KeyCommand<K, V> {
        protected final V value;

        public KeyValueCommand(K key, V value) {
            super(key);
            this.value = value;
        }

        public V value() {
            return value;
        }

        public static abstract class Builder<B extends Builder<B, E, K, V>, E extends KeyValueCommand<K, V>, K extends Streamable, V extends Streamable>
            extends KeyCommand.Builder<B, E, K, V> {
            protected V value;

            @Override
            public B from(E entry) {
                value = entry.value;
                return super.from(entry);
            }

            @SuppressWarnings("unchecked")
            public B setValue(V value) {
                this.value = value;
                return (B) this;
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
                super.readFrom(stream);
                value = stream.readStreamable();
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
                super.writeTo(stream);
                stream.writeClass(value.getClass());
                stream.writeStreamable(value);
            }
        }
    }

    public static abstract class TtlCommand<K extends Streamable, V extends Streamable> extends KeyValueCommand<K, V> {
        protected final Mode mode;
        protected final long ttl;

        public TtlCommand(K key, V value, Mode mode, long ttl) {
            super(key, value);
            this.mode = mode;
            this.ttl = ttl;
        }

        public Mode mode() {
            return mode;
        }

        public long ttl() {
            return ttl;
        }

        public static abstract class Builder<B extends Builder<B, E, K, V>, E extends TtlCommand<K, V>, K extends Streamable, V extends Streamable>
            extends KeyValueCommand.Builder<B, E, K, V> {

            protected Mode mode = Mode.PERSISTENT;
            protected long ttl;

            @Override
            public B from(E entry) {
                mode = entry.mode;
                ttl = entry.ttl;
                return super.from(entry);
            }

            @SuppressWarnings("unchecked")
            public B setMode(Mode mode) {
                this.mode = mode;
                return (B) this;
            }

            @SuppressWarnings("unchecked")
            public B setTtl(long ttl) {
                this.ttl = ttl;
                return (B) this;
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
                super.readFrom(stream);
                mode = stream.readEnum(Mode.class);
                ttl = stream.readLong();
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
                super.writeTo(stream);
                stream.writeEnum(mode);
                stream.writeLong(ttl);
            }
        }
    }

    public static class ContainsKey<K extends Streamable> extends KeyQuery<K, BooleanResult> implements Entry<ContainsKey<K>> {
        public ContainsKey(ConsistencyLevel consistency, K key) {
            super(consistency, key);
        }

        @Override
        public Builder<K> toBuilder() {
            return new Builder<K>().from(this);
        }

        public static <K extends Streamable> Builder<K> builder() {
            return new Builder<>();
        }

        public static class Builder<K extends Streamable> extends KeyQuery.Builder<Builder<K>, ContainsKey<K>, K, BooleanResult> {

            @Override
            public ContainsKey<K> build() {
                return new ContainsKey<>(consistency, key);
            }
        }
    }

    public static class Put<K extends Streamable, V extends Streamable> extends TtlCommand<K, V> implements Entry<Put<K, V>> {
        public Put(K key, V value, Mode mode, long ttl) {
            super(key, value, mode, ttl);
        }

        @Override
        public Builder<K, V> toBuilder() {
            return new Builder<K, V>().from(this);
        }

        public static <K extends Streamable, V extends Streamable> Builder<K, V> builder() {
            return new Builder<>();
        }

        public static class Builder<K extends Streamable, V extends Streamable>
            extends TtlCommand.Builder<Builder<K, V>, Put<K, V>, K, V> {

            @Override
            public Put<K, V> build() {
                return new Put<>(key, value, mode, ttl);
            }
        }
    }

    public static class PutIfAbsent<K extends Streamable, V extends Streamable> extends TtlCommand<K, V> implements Entry<PutIfAbsent<K, V>> {

        public PutIfAbsent(K key, V value, Mode mode, long ttl) {
            super(key, value, mode, ttl);
        }

        @Override
        public Builder<K, V> toBuilder() {
            return new Builder<K, V>().from(this);
        }

        public static <K extends Streamable, V extends Streamable> Builder<K, V> builder() {
            return new Builder<>();
        }

        public static class Builder<K extends Streamable, V extends Streamable> extends TtlCommand.Builder<Builder<K, V>, PutIfAbsent<K, V>, K, V> {

            @Override
            public PutIfAbsent<K, V> build() {
                return new PutIfAbsent<>(key, value, mode, ttl);
            }
        }
    }

    public static class Get<K extends Streamable, V extends Streamable> extends KeyQuery<K, V> implements Entry<Get<K, V>> {

        public Get(ConsistencyLevel consistency, K key) {
            super(consistency, key);
        }

        @Override
        public Builder<K, V> toBuilder() {
            return new Builder<K, V>().from(this);
        }

        public static <K extends Streamable, V extends Streamable> Builder<K, V> builder() {
            return new Builder<>();
        }

        public static class Builder<K extends Streamable, V extends Streamable> extends KeyQuery.Builder<Builder<K, V>, Get<K, V>, K, V> {

            @Override
            public Get<K, V> build() {
                return new Get<>(consistency, key);
            }
        }
    }

    public static class GetOrDefault<K extends Streamable, V extends Streamable> extends KeyQuery<K, V> implements Entry<GetOrDefault<K, V>> {
        private final V defaultValue;

        public GetOrDefault(ConsistencyLevel consistency, K key, V defaultValue) {
            super(consistency, key);
            this.defaultValue = defaultValue;
        }

        public V defaultValue() {
            return defaultValue;
        }

        @Override
        public Builder<K, V> toBuilder() {
            return new Builder<K, V>().from(this);
        }

        public static <K extends Streamable, V extends Streamable> Builder<K, V> builder() {
            return new Builder<>();
        }

        public static class Builder<K extends Streamable, V extends Streamable>
            extends KeyQuery.Builder<Builder<K, V>, GetOrDefault<K, V>, K, V> {
            private V defaultValue;

            @Override
            public Builder<K, V> from(GetOrDefault<K, V> entry) {
                return super.from(entry);
            }

            public Builder<K, V> setDefaultValue(V defaultValue) {
                this.defaultValue = defaultValue;
                return this;
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
                super.readFrom(stream);
                defaultValue = stream.readStreamable();
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
                super.writeTo(stream);
                stream.writeClass(defaultValue.getClass());
                stream.writeStreamable(defaultValue);
            }

            @Override
            public GetOrDefault<K, V> build() {
                return new GetOrDefault<>(consistency, key, defaultValue);
            }
        }
    }

    public static class Remove<K extends Streamable, V extends Streamable> extends KeyValueCommand<K, V> implements Entry<Remove<K, V>> {
        public Remove(K key, V value) {
            super(key, value);
        }

        @Override
        public Builder<K, V> toBuilder() {
            return new Builder<K, V>().from(this);
        }

        public static <K extends Streamable, V extends Streamable> Builder<K, V> builder() {
            return new Builder<>();
        }

        public static class Builder<K extends Streamable, V extends Streamable> extends KeyValueCommand.Builder<Builder<K, V>, Remove<K, V>, K, V> {
            @Override
            public Remove<K, V> build() {
                return new Remove<>(key, value);
            }
        }
    }

    public static class IsEmpty extends MapQuery<BooleanResult> implements Entry<IsEmpty> {
        public IsEmpty(ConsistencyLevel consistency) {
            super(consistency);
        }

        @Override
        public Builder toBuilder() {
            return new Builder().from(this);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder extends MapQuery.Builder<Builder, IsEmpty, BooleanResult> {

            @Override
            public IsEmpty build() {
                return new IsEmpty(consistency);
            }
        }
    }

    public static class Size extends MapQuery<IntegerResult> implements Entry<Size> {
        public Size(ConsistencyLevel consistency) {
            super(consistency);
        }

        @Override
        public Builder toBuilder() {
            return new Builder().from(this);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder extends MapQuery.Builder<Builder, Size, IntegerResult> {

            @Override
            public Size build() {
                return new Size(consistency);
            }
        }
    }

    public static class Clear extends MapCommand<VoidResult> implements Entry<Clear> {

        @Override
        public Builder toBuilder() {
            return new Builder().from(this);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder implements EntryBuilder<Clear> {

            public Builder from(Clear clear) {
                return this;
            }

            @Override
            public Clear build() {
                return new Clear();
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
            }
        }
    }

    public static class StateMachine<K extends Streamable, V extends Streamable> extends org.mitallast.queue.raft.StateMachine {
        private final Map<K, Commit<? extends TtlCommand<K, V>>> map = new HashMap<>();
        private final Set<Long> sessions = new HashSet<>();
        private long time;

        public StateMachine(Settings settings) {
            super(settings);
        }

        private void updateTime(Commit<?> commit) {
            time = Math.max(time, commit.timestamp());
        }

        @Override
        public void sessionRegister(Session session) {
            sessions.add(session.id());
        }

        @Override
        public void sessionExpire(Session session) {
            sessions.remove(session.id());
        }

        @Override
        public void sessionClose(Session session) {
            sessions.remove(session.id());
        }

        private boolean isActive(Commit<? extends TtlCommand> commit) {
            if (commit == null) {
                return false;
            } else if (commit.operation().mode() == Mode.EPHEMERAL && !sessions.contains(commit.session().id())) {
                return false;
            } else if (commit.operation().ttl() != 0 && commit.operation().ttl() < time - commit.timestamp()) {
                return false;
            }
            return true;
        }

        @Apply(ContainsKey.class)
        public BooleanResult containsKey(Commit<ContainsKey<K>> commit) {
            updateTime(commit);
            Commit<? extends TtlCommand> command = map.get(commit.operation().key());
            if (!isActive(command)) {
                map.remove(commit.operation().key());
                return new BooleanResult(false);
            }
            return new BooleanResult(true);
        }

        @Apply(Get.class)
        public V get(Commit<Get<K, V>> commit) {
            updateTime(commit);
            Commit<? extends TtlCommand<K, V>> command = map.get(commit.operation().key());
            if (command != null) {
                if (!isActive(command)) {
                    map.remove(commit.operation().key());
                } else {
                    return command.operation().value();
                }
            }
            return null;
        }

        @Apply(GetOrDefault.class)
        public V getOrDefault(Commit<GetOrDefault<K, V>> commit) {
            updateTime(commit);
            Commit<? extends TtlCommand<K, V>> command = map.get(commit.operation().key());
            if (command == null) {
                return commit.operation().defaultValue();
            } else if (!isActive(command)) {
                map.remove(commit.operation().key());
            } else {
                return command.operation().value();
            }
            return commit.operation().defaultValue();
        }

        @Apply(Put.class)
        public V put(Commit<Put<K, V>> commit) {
            updateTime(commit);
            Commit<? extends TtlCommand<K, V>> put = map.put(commit.operation().key(), commit);
            if (put != null) {
                return put.operation().value;
            } else {
                return null;
            }
        }

        @Apply(PutIfAbsent.class)
        public V putIfAbsent(Commit<PutIfAbsent<K, V>> commit) {
            updateTime(commit);
            Commit<? extends TtlCommand<K, V>> put = map.putIfAbsent(commit.operation().key(), commit);
            if (put != null) {
                return put.operation().value;
            } else {
                return null;
            }
        }

        @Filter({Put.class, PutIfAbsent.class})
        public boolean filterPut(Commit<? extends TtlCommand<K, V>> commit) {
            Commit<? extends TtlCommand<K, V>> command = map.get(commit.operation().key());
            return command != null && command.index() == commit.index() && isActive(command);
        }

        @Apply(Remove.class)
        public V remove(Commit<Remove<K, V>> commit) {
            updateTime(commit);
            if (commit.operation().value() != null) {
                Commit<? extends TtlCommand<K, V>> command = map.get(commit.operation().key());
                if (!isActive(command)) {
                    map.remove(commit.operation().key());
                } else {
                    Object value = command.operation().value();
                    if ((value == null && commit.operation().value() == null) || (value != null && commit.operation().value() != null && value.equals(commit.operation().value()))) {
                        Commit<? extends TtlCommand<K, V>> remove = map.remove(commit.operation().key());
                        if (remove != null) {
                            return remove.operation().value;
                        } else {
                            return null;
                        }
                    }
                    return null;
                }
                return null;
            } else {
                Commit<? extends TtlCommand<K, V>> command = map.remove(commit.operation().key());
                return isActive(command) ? command.operation().value() : null;
            }
        }

        @Filter(value = {Remove.class, Clear.class}, compaction = Compaction.Type.MAJOR)
        public boolean filterRemove(Commit<?> commit, Compaction compaction) {
            return commit.index() > compaction.index();
        }

        @Apply(Size.class)
        public IntegerResult size(Commit<Size> commit) {
            updateTime(commit);
            return new IntegerResult(map.size());
        }

        @Apply(IsEmpty.class)
        public BooleanResult isEmpty(Commit<IsEmpty> commit) {
            updateTime(commit);
            return new BooleanResult(map.isEmpty());
        }

        @Apply(Clear.class)
        public VoidResult clear(Commit<Clear> commit) {
            updateTime(commit);
            map.clear();
            return new VoidResult();
        }
    }
}
