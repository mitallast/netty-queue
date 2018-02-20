package org.mitallast.queue.crdt

import io.vavr.collection.HashMap
import io.vavr.collection.Map
import io.vavr.concurrent.Future
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.common.events.EventBus
import org.mitallast.queue.crdt.bucket.Bucket
import org.mitallast.queue.crdt.bucket.BucketFactory
import org.mitallast.queue.crdt.event.ClosedLogSynced
import org.mitallast.queue.crdt.protocol.AppendEntries
import org.mitallast.queue.crdt.protocol.AppendRejected
import org.mitallast.queue.crdt.protocol.AppendSuccessful
import org.mitallast.queue.crdt.routing.ResourceType
import org.mitallast.queue.crdt.routing.RoutingBucket
import org.mitallast.queue.crdt.routing.RoutingReplica
import org.mitallast.queue.crdt.routing.RoutingTable
import org.mitallast.queue.crdt.routing.allocation.AllocationStrategy
import org.mitallast.queue.crdt.routing.event.RoutingTableChanged
import org.mitallast.queue.crdt.routing.fsm.*
import org.mitallast.queue.raft.Raft
import org.mitallast.queue.raft.RaftState.Leader
import org.mitallast.queue.raft.cluster.ClusterDiscovery
import org.mitallast.queue.raft.event.MembersChanged
import org.mitallast.queue.raft.protocol.ClientMessage
import org.mitallast.queue.transport.TransportController
import org.mitallast.queue.transport.TransportService
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import javax.inject.Inject

class DefaultCrdtService @Inject constructor(
    private val raft: Raft,
    private val routingTableFSM: RoutingTableFSM,
    private val allocationStrategy: AllocationStrategy,
    private val discovery: ClusterDiscovery,
    private val bucketFactory: BucketFactory,
    private val transportService: TransportService,
    transportController: TransportController,
    eventBus: EventBus
) : CrdtService {
    private val logger = LogManager.getLogger()

    private val lock = ReentrantLock()
    @Volatile private var lastApplied: Long = 0
    @Volatile private var buckets: Map<Int, Bucket> = HashMap.empty()

    init {

        val executor = Executors.newSingleThreadExecutor()
        eventBus.subscribe(MembersChanged::class.java, this::handle, executor)
        eventBus.subscribe(RoutingTableChanged::class.java, this::handle, executor)
        eventBus.subscribe(ClosedLogSynced::class.java, this::handle, executor)

        transportController.registerMessageHandler(AppendEntries::class.java) { message: AppendEntries -> this.append(message) }
        transportController.registerMessageHandler(AppendSuccessful::class.java) { message: AppendSuccessful -> this.successful(message) }
        transportController.registerMessageHandler(AppendRejected::class.java) { message: AppendRejected -> this.rejected(message) }
    }

    private fun append(message: AppendEntries) {
        val bucket = bucket(message.bucket)
        if (bucket == null) {
            logger.warn("unexpected bucket {}, ignore", message.bucket)
        } else {
            bucket.lock().lock()
            try {
                val routingBucket = routingTable().bucket(message.bucket.toLong())
                val replica = routingBucket.replicas.getOrElse(message.replica, null)
                if (replica == null) {
                    logger.warn("unexpected replica {}, ignore", message.replica)
                } else {
                    var localIndex = bucket.state()[message.replica]
                    if (localIndex == message.prevIndex) {
                        for (logEntry in message.entries) {
                            bucket.registry().crdt(logEntry.id).update(logEntry.event)
                            localIndex = Math.max(localIndex, logEntry.index)
                        }
                        bucket.state().put(message.replica, localIndex)
                        if (logger.isDebugEnabled) {
                            logger.debug("[replica={}:{}] append success to={}:{} prev={} new={}",
                                bucket.index(), bucket.replica(),
                                message.bucket, message.replica, message.prevIndex, localIndex)
                        }
                        transportService.send(
                            replica.member,
                            AppendSuccessful(message.bucket, bucket.replica(), localIndex)
                        )
                    } else {
                        logger.warn("[replica={}:{}] append reject to={}:{} prev={} index={}",
                            bucket.index(), bucket.replica(),
                            message.bucket, message.replica, message.prevIndex, localIndex)
                        transportService.send(
                            replica.member,
                            AppendRejected(message.bucket, bucket.replica(), localIndex)
                        )
                    }
                }
            } finally {
                bucket.lock().unlock()
            }
        }
    }

    private fun successful(message: AppendSuccessful) {
        val bucket = bucket(message.bucket)
        bucket?.replicator()?.successful(message)
    }

    private fun rejected(message: AppendRejected) {
        val bucket = bucket(message.bucket)
        bucket?.replicator()?.rejected(message)
    }

    private fun handle(message: ClosedLogSynced) {
        lock.lock()
        try {
            val routingTable = routingTableFSM.get()
            val replica = routingTable.bucket(message.bucket.toLong()).replicas.get(message.replica)
            if (replica.isDefined && replica.get().isClosed) {
                logger.info("RemoveReplica bucket {} {}", message.bucket, message.replica)
                val request = RemoveReplica(message.bucket, message.replica)
                raft.apply(ClientMessage(request, 0))
            } else {
                logger.warn("open closed replicator bucket {}", message.bucket)
                val bucket = bucket(message.bucket)
                bucket?.replicator()?.open()
            }
        } finally {
            lock.unlock()
        }
    }

    override fun routingTable(): RoutingTable {
        return routingTableFSM.get()
    }

    operator fun contains(index: Int): Boolean {
        return buckets.containsKey(index)
    }

    override fun bucket(index: Int): Bucket? {
        return buckets.getOrElse(index, null)
    }

    override fun bucket(resourceId: Long): Bucket? {
        val index = routingTable().bucket(resourceId).index
        return bucket(index)
    }

    override fun addResource(id: Long, resourceType: ResourceType): Future<Boolean> {
        return raft.command(AddResource(id, resourceType))
            .filter { m -> m is AddResourceResponse }
            .map { m -> (m as AddResourceResponse).isCreated }
    }

    private fun handle(event: MembersChanged) {
        if (raft.currentState() === Leader) {
            logger.info("members changed")
            raft.apply(ClientMessage(UpdateMembers(event.members), 0))
        }
    }

    private fun handle(changed: RoutingTableChanged) {
        lock.lock()
        try {
            if (changed.index <= lastApplied) {
                return
            }
            lastApplied = changed.index
            logger.info("routing table changed: {}", changed.routingTable)
            processAsLeader(changed.routingTable)
            processBuckets(changed.routingTable)
        } finally {
            lock.unlock()
        }
    }

    private fun processAsLeader(routingTable: RoutingTable) {
        if (raft.currentState() === Leader) {
            allocationStrategy.update(routingTable)
                .forEach { cmd -> raft.apply(ClientMessage(cmd, 0)) }
        }
    }

    private fun processBuckets(routingTable: RoutingTable) {
        for (routingBucket in routingTable.buckets) {
            val replicaOpt = routingBucket.replicas.values()
                .find { r -> r.member == discovery.self }
            if (replicaOpt.isDefined) {
                processReplica(routingBucket, replicaOpt.get())
            } else {
                deleteIfExists(routingBucket.index)
            }
        }
    }

    private fun processReplica(routingBucket: RoutingBucket, replica: RoutingReplica) {
        var bucket = bucket(routingBucket.index)
        if (bucket == null) {
            bucket = bucketFactory.create(routingBucket.index, replica.id)
            buckets = buckets.put(routingBucket.index, bucket)
        }
        bucket.lock().lock()
        try {
            if (replica.isClosed) {
                bucket.replicator().closeAndSync()
            } else {
                bucket.replicator().open()
                for (resource in routingBucket.resources.values()) {
                    if (bucket.registry().crdtOpt(resource.id).isEmpty) {
                        logger.info("allocate resource {}:{}", resource.id, resource.type)
                        when (resource.type) {
                            ResourceType.LWWRegister -> bucket.registry().createLWWRegister(resource.id)
                            ResourceType.GCounter -> bucket.registry().createGCounter(resource.id)
                            ResourceType.GSet -> bucket.registry().createGSet(resource.id)
                            ResourceType.OrderedGSet -> bucket.registry().createOrderedGSet(resource.id)
                        }
                    }
                }
            }
        } finally {
            bucket.lock().unlock()
        }
    }

    private fun deleteIfExists(index: Int) {
        val bucket = bucket(index)
        if (bucket != null) {
            logger.info("delete bucket {}", index)
            bucket.lock().lock()
            try {
                buckets = buckets.remove(index)
                bucket.close()
                bucket.delete()
            } finally {
                bucket.lock().unlock()
            }
        }
    }
}
