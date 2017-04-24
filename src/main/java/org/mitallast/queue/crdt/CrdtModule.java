package org.mitallast.queue.crdt;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.Multibinder;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.bucket.BucketFactory;
import org.mitallast.queue.crdt.bucket.DefaultBucket;
import org.mitallast.queue.crdt.commutative.GCounter;
import org.mitallast.queue.crdt.commutative.GSet;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.crdt.log.FileReplicatedLog;
import org.mitallast.queue.crdt.log.ReplicatedLog;
import org.mitallast.queue.crdt.log.ReplicatedLogFactory;
import org.mitallast.queue.crdt.protocol.AppendEntries;
import org.mitallast.queue.crdt.protocol.AppendRejected;
import org.mitallast.queue.crdt.protocol.AppendSuccessful;
import org.mitallast.queue.crdt.registry.CrdtRegistry;
import org.mitallast.queue.crdt.registry.CrdtRegistryFactory;
import org.mitallast.queue.crdt.registry.DefaultCrdtRegistry;
import org.mitallast.queue.crdt.replication.DefaultReplicator;
import org.mitallast.queue.crdt.replication.Replicator;
import org.mitallast.queue.crdt.replication.ReplicatorFactory;
import org.mitallast.queue.crdt.routing.Resource;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.allocation.AllocationStrategy;
import org.mitallast.queue.crdt.routing.allocation.DefaultAllocationStrategy;
import org.mitallast.queue.crdt.routing.fsm.*;
import org.mitallast.queue.crdt.vclock.FileVectorClock;
import org.mitallast.queue.crdt.vclock.VectorClock;
import org.mitallast.queue.crdt.vclock.VectorClockFactory;

import static org.mitallast.queue.common.stream.StreamableRegistry.of;

public class CrdtModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DefaultCrdtService.class).asEagerSingleton();
        bind(CrdtService.class).to(DefaultCrdtService.class);

        // routing

        bind(RoutingTableFSM.class).asEagerSingleton();

        // allocation

        bind(DefaultAllocationStrategy.class).asEagerSingleton();
        bind(AllocationStrategy.class).to(DefaultAllocationStrategy.class);


        // bucket

        install(new FactoryModuleBuilder()
            .implement(ReplicatedLog.class, FileReplicatedLog.class)
            .build(ReplicatedLogFactory.class));

        install(new FactoryModuleBuilder()
            .implement(VectorClock.class, FileVectorClock.class)
            .build(VectorClockFactory.class));

        install(new FactoryModuleBuilder()
            .implement(CrdtRegistry.class, DefaultCrdtRegistry.class)
            .build(CrdtRegistryFactory.class));

        install(new FactoryModuleBuilder()
            .implement(Bucket.class, DefaultBucket.class)
            .build(BucketFactory.class));

        // replication

        install(new FactoryModuleBuilder()
            .implement(Replicator.class, DefaultReplicator.class)
            .build(ReplicatorFactory.class));

        // protocol

        Multibinder<StreamableRegistry> binder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);

        binder.addBinding().toInstance(of(LWWRegister.SourceAssign.class, LWWRegister.SourceAssign::new, 1100));
        binder.addBinding().toInstance(of(LWWRegister.DownstreamAssign.class, LWWRegister.DownstreamAssign::new, 1101));

        binder.addBinding().toInstance(of(GCounter.SourceAssign.class, GCounter.SourceAssign::new, 1110));
        binder.addBinding().toInstance(of(GCounter.DownstreamAssign.class, GCounter.DownstreamAssign::new, 1111));

        binder.addBinding().toInstance(of(GSet.SourceAdd.class, GSet.SourceAdd::new, 1120));
        binder.addBinding().toInstance(of(GSet.DownstreamAdd.class, GSet.DownstreamAdd::new, 1121));

        binder.addBinding().toInstance(of(AppendEntries.class, AppendEntries::new, 1300));
        binder.addBinding().toInstance(of(AppendSuccessful.class, AppendSuccessful::new, 1301));
        binder.addBinding().toInstance(of(AppendRejected.class, AppendRejected::new, 1302));

        binder.addBinding().toInstance(of(Resource.class, Resource::new, 1400));
        binder.addBinding().toInstance(of(RoutingTable.class, RoutingTable::new, 1401));

        binder.addBinding().toInstance(of(AddResource.class, AddResource::new, 1500));
        binder.addBinding().toInstance(of(AddResourceResponse.class, AddResourceResponse::new, 1501));
        binder.addBinding().toInstance(of(RemoveResource.class, RemoveResource::new, 1502));
        binder.addBinding().toInstance(of(RemoveResourceResponse.class, RemoveResourceResponse::new, 1503));
        binder.addBinding().toInstance(of(UpdateMembers.class, UpdateMembers::new, 1504));
        binder.addBinding().toInstance(of(AddReplica.class, AddReplica::new, 1505));
        binder.addBinding().toInstance(of(CloseReplica.class, CloseReplica::new, 1506));
        binder.addBinding().toInstance(of(RemoveReplica.class, RemoveReplica::new, 1507));
    }
}
