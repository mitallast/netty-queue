package org.mitallast.queue.crdt;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.crdt.log.DefaultCompactionFilter;
import org.mitallast.queue.crdt.log.FileReplicatedLog;
import org.mitallast.queue.crdt.log.LogEntry;
import org.mitallast.queue.crdt.protocol.AppendEntries;
import org.mitallast.queue.crdt.protocol.AppendRejected;
import org.mitallast.queue.crdt.protocol.AppendSuccessful;
import org.mitallast.queue.crdt.replication.Replica;
import org.mitallast.queue.crdt.replication.Replicator;
import org.mitallast.queue.crdt.vclock.FileVectorClock;
import org.mitallast.queue.crdt.log.ReplicatedLog;
import org.mitallast.queue.crdt.vclock.VectorClock;

import java.util.function.Predicate;

import static org.mitallast.queue.common.stream.StreamableRegistry.of;

public class CrdtModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DefaultCrdtService.class).asEagerSingleton();
        bind(DefaultCompactionFilter.class).asEagerSingleton();
        bind(FileReplicatedLog.class).asEagerSingleton();
        bind(FileVectorClock.class).asEagerSingleton();
        bind(Replicator.class).asEagerSingleton();
        bind(Replica.class).asEagerSingleton();

        bind(ReplicatedLog.class).to(FileReplicatedLog.class);
        bind(VectorClock.class).to(FileVectorClock.class);
        bind(CrdtService.class).to(DefaultCrdtService.class);

        bind(new TypeLiteral<Predicate<LogEntry>>() {}).to(DefaultCompactionFilter.class);

        Multibinder<StreamableRegistry> binder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);

        binder.addBinding().toInstance(of(LWWRegister.SourceAssign.class, LWWRegister.SourceAssign::new, 1000));
        binder.addBinding().toInstance(of(LWWRegister.DownstreamAssign.class, LWWRegister.DownstreamAssign::new, 1001));
        binder.addBinding().toInstance(of(LWWRegister.Value.class, LWWRegister.Value::read, 1002));
        binder.addBinding().toInstance(of(LWWRegister.ValueResponse.class, LWWRegister.ValueResponse::new, 1003));

        binder.addBinding().toInstance(of(AppendEntries.class, AppendEntries::new, 1004));
        binder.addBinding().toInstance(of(AppendSuccessful.class, AppendSuccessful::new, 1005));
        binder.addBinding().toInstance(of(AppendRejected.class, AppendRejected::new, 1006));
    }
}
