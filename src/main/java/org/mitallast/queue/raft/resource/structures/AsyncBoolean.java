package org.mitallast.queue.raft.resource.structures;

import com.google.inject.Inject;
import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.*;
import org.mitallast.queue.raft.log.compaction.Compaction;
import org.mitallast.queue.raft.resource.AbstractResource;
import org.mitallast.queue.raft.resource.Stateful;
import org.mitallast.queue.raft.resource.result.BooleanResult;
import org.mitallast.queue.raft.resource.result.VoidResult;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Stateful(AsyncBoolean.StateMachine.class)
public class AsyncBoolean extends AbstractResource {

    public AsyncBoolean(Protocol protocol) {
        super(protocol);
    }

    public CompletableFuture<Boolean> get() {
        return submit(Get.builder().build()).thenApply(BooleanResult::get);
    }

    public CompletableFuture<Void> set(boolean value) {
        return submit(Set.builder().setValue(value).build()).thenApply(voidResult -> null);
    }

    public CompletableFuture<Boolean> getAndSet(boolean value) {
        return submit(GetAndSet.builder().setValue(value).build()).thenApply(BooleanResult::get);
    }

    public CompletableFuture<Boolean> compareAndSet(boolean expect, boolean update) {
        return submit(CompareAndSet.builder().setExpect(expect).setUpdate(update).build()).thenApply(BooleanResult::get);
    }

    public static abstract class BooleanCommand<V extends Streamable> implements Command<V> {
    }

    public static abstract class BooleanQuery<V extends Streamable> implements Query<V> {

        @Override
        public ConsistencyLevel consistency() {
            return ConsistencyLevel.LINEARIZABLE_STRICT;
        }
    }

    public static class Get extends BooleanQuery<BooleanResult> implements Entry<Get> {
        @Override
        public Builder toBuilder() {
            return new Builder().from(this);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder implements EntryBuilder<Get> {

            @Override
            public Get build() {
                return new Get();
            }

            public Builder from(Get get) {
                return this;
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
            }
        }
    }

    public static class Set extends BooleanCommand<VoidResult> implements Entry<Set> {
        private final boolean value;

        public Set(boolean value) {
            this.value = value;
        }

        public boolean value() {
            return value;
        }

        @Override
        public Builder toBuilder() {
            return new Builder().from(this);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder implements EntryBuilder<Set> {
            private boolean value;

            public Builder from(Set get) {
                return this;
            }

            public Builder setValue(boolean value) {
                this.value = value;
                return this;
            }

            @Override
            public Set build() {
                return new Set(value);
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
                value = stream.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
                stream.writeBoolean(value);
            }
        }
    }

    public static class CompareAndSet extends BooleanCommand<BooleanResult> implements Entry<CompareAndSet> {
        private final boolean expect;
        private final boolean update;

        public CompareAndSet(boolean expect, boolean update) {
            this.expect = expect;
            this.update = update;
        }

        public boolean expect() {
            return expect;
        }

        public boolean update() {
            return update;
        }

        @Override
        public Builder toBuilder() {
            return new Builder().from(this);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder implements EntryBuilder<CompareAndSet> {
            private boolean expect;
            private boolean update;

            public Builder from(CompareAndSet entry) {
                expect = entry.expect;
                update = entry.update;
                return this;
            }

            public Builder setExpect(boolean expect) {
                this.expect = expect;
                return this;
            }

            public Builder setUpdate(boolean update) {
                this.update = update;
                return this;
            }

            @Override
            public CompareAndSet build() {
                return new CompareAndSet(expect, update);
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
                expect = stream.readBoolean();
                update = stream.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
                stream.writeBoolean(expect);
                stream.writeBoolean(update);
            }
        }
    }

    public static class GetAndSet extends BooleanCommand<BooleanResult> implements Entry<GetAndSet> {
        private final boolean value;

        public GetAndSet(boolean value) {
            this.value = value;
        }

        public boolean value() {
            return value;
        }

        @Override
        public Builder toBuilder() {
            return new Builder().from(this);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder implements EntryBuilder<GetAndSet> {
            private boolean value;

            public Builder from(GetAndSet get) {
                return this;
            }

            public Builder setValue(boolean value) {
                this.value = value;
                return this;
            }

            @Override
            public GetAndSet build() {
                return new GetAndSet(value);
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
                value = stream.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
                stream.writeBoolean(value);
            }
        }
    }

    public static class StateMachine extends org.mitallast.queue.raft.StateMachine {
        private final AtomicBoolean value = new AtomicBoolean();
        private volatile long version;

        @Inject
        public StateMachine(Settings settings) {
            super(settings);
        }

        @Apply(Get.class)
        protected BooleanResult get(Commit<Get> commit) {
            return new BooleanResult(value.get());
        }

        @Apply(Set.class)
        protected VoidResult set(Commit<Set> commit) {
            value.set(commit.operation().value());
            version = commit.index();
            return new VoidResult();
        }

        @Apply(CompareAndSet.class)
        protected BooleanResult compareAndSet(Commit<CompareAndSet> commit) {
            return new BooleanResult(value.compareAndSet(commit.operation().expect(), commit.operation().update()));
        }

        @Apply(GetAndSet.class)
        protected BooleanResult getAndSet(Commit<GetAndSet> commit) {
            boolean result = value.getAndSet(commit.operation().value());
            version = commit.index();
            return new BooleanResult(result);
        }

        @Filter(Filter.All.class)
        protected boolean filterAll(Commit<? extends BooleanCommand<?>> commit, Compaction compaction) {
            return commit.index() >= version;
        }
    }
}
