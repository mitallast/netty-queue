package org.mitallast.queue.crdt;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.bucket.BucketFactory;
import org.mitallast.queue.crdt.bucket.DefaultBucket;
import org.mitallast.queue.crdt.commutative.GCounter;
import org.mitallast.queue.crdt.commutative.GSet;
import org.mitallast.queue.crdt.commutative.LWWRegister;
import org.mitallast.queue.crdt.commutative.OrderedGSet;
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
import org.mitallast.queue.crdt.replication.state.FileReplicaState;
import org.mitallast.queue.crdt.replication.state.ReplicaState;
import org.mitallast.queue.crdt.replication.state.ReplicaStateFactory;
import org.mitallast.queue.crdt.routing.Resource;
import org.mitallast.queue.crdt.routing.RoutingTable;
import org.mitallast.queue.crdt.routing.allocation.AllocationStrategy;
import org.mitallast.queue.crdt.routing.allocation.DefaultAllocationStrategy;
import org.mitallast.queue.crdt.routing.fsm.*;

public class CrdtModule extends AbstractModule {
    static {
        Codec.Companion.register(300, LWWRegister.SourceAssign.class, LWWRegister.SourceAssign.codec);
        Codec.Companion.register(301, LWWRegister.DownstreamAssign.class, LWWRegister.DownstreamAssign.codec);
        Codec.Companion.register(302, GCounter.SourceAssign.class, GCounter.SourceAssign.codec);
        Codec.Companion.register(303, GCounter.DownstreamAssign.class, GCounter.DownstreamAssign.codec);
        Codec.Companion.register(304, GSet.SourceAdd.class, GSet.SourceAdd.codec);
        Codec.Companion.register(305, GSet.DownstreamAdd.class, GSet.DownstreamAdd.codec);
        Codec.Companion.register(306, OrderedGSet.SourceAdd.class, OrderedGSet.SourceAdd.codec);
        Codec.Companion.register(307, OrderedGSet.DownstreamAdd.class, OrderedGSet.DownstreamAdd.codec);
        Codec.Companion.register(308, AppendEntries.class, AppendEntries.codec);
        Codec.Companion.register(309, AppendSuccessful.class, AppendSuccessful.codec);
        Codec.Companion.register(310, AppendRejected.class, AppendRejected.codec);
        Codec.Companion.register(311, Resource.class, Resource.codec);
        Codec.Companion.register(312, RoutingTable.class, RoutingTable.codec);
        Codec.Companion.register(313, AddResource.class, AddResource.codec);
        Codec.Companion.register(314, AddResourceResponse.class, AddResourceResponse.codec);
        Codec.Companion.register(315, RemoveResource.class, RemoveResource.codec);
        Codec.Companion.register(316, RemoveResourceResponse.class, RemoveResourceResponse.codec);
        Codec.Companion.register(317, UpdateMembers.class, UpdateMembers.codec);
        Codec.Companion.register(318, AddReplica.class, AddReplica.codec);
        Codec.Companion.register(319, CloseReplica.class, CloseReplica.codec);
        Codec.Companion.register(320, RemoveReplica.class, RemoveReplica.codec);
    }

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
            .implement(ReplicaState.class, FileReplicaState.class)
            .build(ReplicaStateFactory.class));

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
    }
}
