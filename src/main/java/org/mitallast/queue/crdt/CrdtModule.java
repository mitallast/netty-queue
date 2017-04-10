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
import org.mitallast.queue.crdt.routing.Resource;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.RoutingService;
import org.mitallast.queue.crdt.routing.fsm.*;
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

        // shard

        bind(RoutingService.class).asEagerSingleton();
        bind(RoutingTableFSM.class).asEagerSingleton();

        Multibinder<StreamableRegistry> binder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);

        binder.addBinding().toInstance(of(LWWRegister.SourceAssign.class, LWWRegister.SourceAssign::new, 1000));
        binder.addBinding().toInstance(of(LWWRegister.DownstreamAssign.class, LWWRegister.DownstreamAssign::new, 1001));

        binder.addBinding().toInstance(of(AppendEntries.class, AppendEntries::new, 1004));
        binder.addBinding().toInstance(of(AppendSuccessful.class, AppendSuccessful::new, 1005));
        binder.addBinding().toInstance(of(AppendRejected.class, AppendRejected::new, 1006));

        binder.addBinding().toInstance(of(Resource.class, Resource::new, 1100));
        binder.addBinding().toInstance(of(RoutingTable.class, RoutingTable::new, 1101));

        binder.addBinding().toInstance(of(AddResource.class, AddResource::new, 1200));
        binder.addBinding().toInstance(of(AddResourceResponse.class, AddResourceResponse::new, 1201));
        binder.addBinding().toInstance(of(RemoveResource.class, RemoveResource::new, 1202));
        binder.addBinding().toInstance(of(RemoveResourceResponse.class, RemoveResourceResponse::new, 1203));
        binder.addBinding().toInstance(of(AddServer.class, AddServer::new, 1204));
        binder.addBinding().toInstance(of(RemoveServer.class, RemoveServer::new, 1205));
        binder.addBinding().toInstance(of(Allocate.class, Allocate::new, 1206));
    }
}
