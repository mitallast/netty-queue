package org.mitallast.queue.crdt.routing.fsm

import com.google.common.base.Preconditions
import com.typesafe.config.Config
import io.vavr.control.Option
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.events.EventBus
import org.mitallast.queue.common.file.FileService
import org.mitallast.queue.crdt.routing.Resource
import org.mitallast.queue.crdt.routing.RoutingTable
import org.mitallast.queue.crdt.routing.event.RoutingTableChanged
import org.mitallast.queue.raft.protocol.RaftSnapshotMetadata
import org.mitallast.queue.raft.resource.ResourceFSM
import org.mitallast.queue.raft.resource.ResourceRegistry
import java.io.File
import java.io.IOError
import java.io.IOException
import javax.inject.Inject

class RoutingTableFSM @Inject constructor(
    config: Config,
    registry: ResourceRegistry,
    private val eventBus: EventBus,
    private val fileService: FileService
) : ResourceFSM {
    private val logger = LogManager.getLogger()
    private val file: File = fileService.resource("crdt", "routing.bin")

    @Volatile private var lastApplied: Long = 0
    @Volatile private var routingTable: RoutingTable = RoutingTable(
        config.getInt("crdt.replicas"),
        config.getInt("crdt.buckets")
    )

    init {
        restore()

        registry.register(this)
        registry.register(AddResource::class.java, this::handle)
        registry.register(RemoveResource::class.java, this::handle)
        registry.register(UpdateMembers::class.java, this::handle)

        registry.register(AddReplica::class.java, this::handle)
        registry.register(CloseReplica::class.java, this::handle)
        registry.register(RemoveReplica::class.java, this::handle)

        registry.register(RoutingTable::class.java, this::handle)
    }

    fun get(): RoutingTable {
        return routingTable
    }

    private fun restore() {
        if (file.length() > 0) {
            try {
                fileService.input(file).use { stream -> routingTable = RoutingTable.codec.read(stream) }
            } catch (e: IOException) {
                throw IOError(e)
            }

        }
    }

    private fun persist(index: Long, routingTable: RoutingTable) {
        Preconditions.checkArgument(index > lastApplied)
        this.lastApplied = index
        logger.info("before: {}", this.routingTable)
        this.routingTable = routingTable
        logger.info("after: {}", this.routingTable)
        try {
            fileService.output(file).use { stream -> RoutingTable.codec.write(stream, routingTable) }
        } catch (e: IOException) {
            throw IOError(e)
        }

        eventBus.trigger(RoutingTableChanged(index, routingTable))
    }

    private fun handle(index: Long, routingTable: RoutingTable): Option<Message> {
        if (index <= lastApplied) {
            return Option.none()
        }
        persist(index, routingTable)
        return Option.none()
    }

    private fun handle(index: Long, request: AddResource): Option<Message> {
        if (index <= lastApplied) {
            return Option.none()
        }
        if (routingTable.hasResource(request.id)) {
            return Option.some(AddResourceResponse(request.type, request.id, false))
        }
        val resource = Resource(
            request.id,
            request.type
        )
        persist(index, routingTable.withResource(resource))
        return Option.some(AddResourceResponse(request.type, request.id, true))
    }

    private fun handle(index: Long, request: RemoveResource): Option<Message> {
        if (index <= lastApplied) {
            return Option.none()
        }
        if (routingTable.hasResource(request.id)) {
            persist(index, routingTable.withoutResource(request.id))
            return Option.some(RemoveResourceResponse(request.type, request.id, true))
        }
        return Option.some(RemoveResourceResponse(request.type, request.id, false))
    }

    private fun handle(index: Long, updateMembers: UpdateMembers): Option<Message> {
        if (index <= lastApplied) {
            return Option.none()
        }
        persist(index, routingTable.withMembers(updateMembers.members))
        return Option.none()
    }

    private fun handle(index: Long, request: AddReplica): Option<Message> {
        if (index <= lastApplied) {
            return Option.none()
        }
        val routingBucket = routingTable.bucket(request.bucket.toLong())
        if (!routingBucket.exists(request.member)) {
            persist(index, routingTable.withReplica(request.bucket, request.member))
        } else {
            logger.warn("node {} already allocated in bucket {}", request.member, request.bucket)
        }
        return Option.none()
    }

    private fun handle(index: Long, request: CloseReplica): Option<Message> {
        if (index <= lastApplied) {
            return Option.none()
        }
        val routingBucket = routingTable.bucket(request.bucket.toLong())
        val replica = routingBucket.replicas.get(request.replica)
        if (replica.exists { it.isOpened }) {
            persist(index, routingTable.withReplica(request.bucket, replica.get().close()))
        }
        return Option.none()
    }

    private fun handle(index: Long, request: RemoveReplica): Option<Message> {
        if (index <= lastApplied) {
            return Option.none()
        }
        val routingBucket = routingTable.bucket(request.bucket.toLong())
        val replica = routingBucket.replicas.get(request.replica)
        if (replica.exists { it.isClosed }) {
            persist(index, routingTable.withoutReplica(request.bucket, request.replica))
        }
        return Option.none()
    }

    override fun prepareSnapshot(snapshotMeta: RaftSnapshotMetadata): Option<Message> {
        return Option.some(routingTable)
    }
}
