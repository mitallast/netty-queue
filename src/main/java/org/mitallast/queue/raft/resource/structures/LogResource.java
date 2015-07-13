package org.mitallast.queue.raft.resource.structures;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.log.Log;
import org.mitallast.queue.log.LogService;
import org.mitallast.queue.log.entry.LogEntry;
import org.mitallast.queue.raft.Apply;
import org.mitallast.queue.raft.Command;
import org.mitallast.queue.raft.Commit;
import org.mitallast.queue.raft.Protocol;
import org.mitallast.queue.raft.resource.AbstractResource;
import org.mitallast.queue.raft.resource.Stateful;
import org.mitallast.queue.raft.resource.result.LongResult;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@Stateful(LogResource.StateMachine.class)
public class LogResource extends AbstractResource {

    public LogResource(Protocol protocol) {
        super(protocol);
    }

    public CompletableFuture<Long> appendEntry(LogEntry entry) {
        return submit(AppendEntry.builder()
            .setLogEntry(entry)
            .build()).thenApply(LongResult::get);
    }

    public static abstract class LogCommand<V extends Streamable> implements Command<V> {
    }

    public static class AppendEntry extends LogCommand<LongResult> implements Entry<AppendEntry> {
        private final LogEntry logEntry;

        public AppendEntry(LogEntry logEntry) {
            this.logEntry = logEntry;
        }

        public LogEntry logEntry() {
            return logEntry;
        }

        @Override
        public Builder toBuilder() {
            return new Builder().from(this);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder implements EntryBuilder<AppendEntry> {
            private LogEntry logEntry;

            public Builder from(AppendEntry entry) {
                logEntry = entry.logEntry;
                return this;
            }

            @Override
            public AppendEntry build() {
                return new AppendEntry(logEntry);
            }

            public Builder setLogEntry(LogEntry logEntry) {
                this.logEntry = logEntry;
                return this;
            }

            @Override
            public void readFrom(StreamInput stream) throws IOException {
                EntryBuilder<LogEntry> entryBuilder = stream.readStreamable();
                logEntry = entryBuilder.build();
            }

            @Override
            public void writeTo(StreamOutput stream) throws IOException {
                EntryBuilder builder = logEntry.toBuilder();
                stream.writeClass(builder.getClass());
                stream.writeStreamable(builder);
            }
        }
    }

    public static class StateMachine extends org.mitallast.queue.raft.StateMachine {
        private final String path;
        private final Log log;

        @Inject
        public StateMachine(Settings settings, @Named("path") String path, LogService logService) throws IOException {
            super(settings);
            this.path = path;
            this.log = logService.log(path);
        }

        @Apply(AppendEntry.class)
        public LongResult appendEntry(Commit<AppendEntry> commit) throws IOException {
            long index = log.nextIndex();
            LogEntry logEntry = (LogEntry) commit.operation().logEntry().toBuilder()
                .setIndex(index)
                .build();
            log.appendEntry(logEntry);
            return new LongResult(index);
        }
    }
}
