package org.mitallast.queue.crdt

import com.google.inject.AbstractModule
import com.google.inject.assistedinject.FactoryModuleBuilder
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.crdt.bucket.Bucket
import org.mitallast.queue.crdt.bucket.BucketFactory
import org.mitallast.queue.crdt.bucket.DefaultBucket
import org.mitallast.queue.crdt.commutative.GCounter
import org.mitallast.queue.crdt.commutative.GSet
import org.mitallast.queue.crdt.commutative.LWWRegister
import org.mitallast.queue.crdt.commutative.OrderedGSet
import org.mitallast.queue.crdt.log.FileReplicatedLog
import org.mitallast.queue.crdt.log.ReplicatedLog
import org.mitallast.queue.crdt.log.ReplicatedLogFactory
import org.mitallast.queue.crdt.protocol.AppendEntries
import org.mitallast.queue.crdt.protocol.AppendRejected
import org.mitallast.queue.crdt.protocol.AppendSuccessful
import org.mitallast.queue.crdt.registry.CrdtRegistry
import org.mitallast.queue.crdt.registry.CrdtRegistryFactory
import org.mitallast.queue.crdt.registry.DefaultCrdtRegistry
import org.mitallast.queue.crdt.replication.DefaultReplicator
import org.mitallast.queue.crdt.replication.Replicator
import org.mitallast.queue.crdt.replication.ReplicatorFactory
import org.mitallast.queue.crdt.replication.state.FileReplicaState
import org.mitallast.queue.crdt.replication.state.ReplicaState
import org.mitallast.queue.crdt.replication.state.ReplicaStateFactory
import org.mitallast.queue.crdt.routing.Resource
import org.mitallast.queue.crdt.routing.RoutingTable
import org.mitallast.queue.crdt.routing.allocation.AllocationStrategy
import org.mitallast.queue.crdt.routing.allocation.DefaultAllocationStrategy
import org.mitallast.queue.crdt.routing.fsm.*

class CrdtModule : AbstractModule() {

    override fun configure() {
        bind(DefaultCrdtService::class.java).asEagerSingleton()
        bind(CrdtService::class.java).to(DefaultCrdtService::class.java)

        // routing

        bind(RoutingTableFSM::class.java).asEagerSingleton()

        // allocation

        bind(DefaultAllocationStrategy::class.java).asEagerSingleton()
        bind(AllocationStrategy::class.java).to(DefaultAllocationStrategy::class.java)

        // bucket

        install(FactoryModuleBuilder()
            .implement(ReplicatedLog::class.java, FileReplicatedLog::class.java)
            .build(ReplicatedLogFactory::class.java))

        install(FactoryModuleBuilder()
            .implement(ReplicaState::class.java, FileReplicaState::class.java)
            .build(ReplicaStateFactory::class.java))

        install(FactoryModuleBuilder()
            .implement(CrdtRegistry::class.java, DefaultCrdtRegistry::class.java)
            .build(CrdtRegistryFactory::class.java))

        install(FactoryModuleBuilder()
            .implement(Bucket::class.java, DefaultBucket::class.java)
            .build(BucketFactory::class.java))

        // replication

        install(FactoryModuleBuilder()
            .implement(Replicator::class.java, DefaultReplicator::class.java)
            .build(ReplicatorFactory::class.java))
    }

    companion object {
        init {
            Codec.register(300, LWWRegister.SourceAssign::class.java, LWWRegister.SourceAssign.codec)
            Codec.register(301, LWWRegister.DownstreamAssign::class.java, LWWRegister.DownstreamAssign.codec)
            Codec.register(302, GCounter.SourceAssign::class.java, GCounter.SourceAssign.codec)
            Codec.register(303, GCounter.DownstreamAssign::class.java, GCounter.DownstreamAssign.codec)
            Codec.register(304, GSet.SourceAdd::class.java, GSet.SourceAdd.codec)
            Codec.register(305, GSet.DownstreamAdd::class.java, GSet.DownstreamAdd.codec)
            Codec.register(306, OrderedGSet.SourceAdd::class.java, OrderedGSet.SourceAdd.codec)
            Codec.register(307, OrderedGSet.DownstreamAdd::class.java, OrderedGSet.DownstreamAdd.codec)
            Codec.register(308, AppendEntries::class.java, AppendEntries.codec)
            Codec.register(309, AppendSuccessful::class.java, AppendSuccessful.codec)
            Codec.register(310, AppendRejected::class.java, AppendRejected.codec)
            Codec.register(311, Resource::class.java, Resource.codec)
            Codec.register(312, RoutingTable::class.java, RoutingTable.codec)
            Codec.register(313, AddResource::class.java, AddResource.codec)
            Codec.register(314, AddResourceResponse::class.java, AddResourceResponse.codec)
            Codec.register(315, RemoveResource::class.java, RemoveResource.codec)
            Codec.register(316, RemoveResourceResponse::class.java, RemoveResourceResponse.codec)
            Codec.register(317, UpdateMembers::class.java, UpdateMembers.codec)
            Codec.register(318, AddReplica::class.java, AddReplica.codec)
            Codec.register(319, CloseReplica::class.java, CloseReplica.codec)
            Codec.register(320, RemoveReplica::class.java, RemoveReplica.codec)
        }
    }
}
