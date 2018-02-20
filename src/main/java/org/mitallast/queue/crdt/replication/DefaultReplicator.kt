package org.mitallast.queue.crdt.replication

import com.google.inject.Inject
import com.google.inject.assistedinject.Assisted
import com.typesafe.config.Config
import gnu.trove.impl.sync.TSynchronizedLongLongMap
import gnu.trove.map.hash.TLongLongHashMap
import io.vavr.collection.Vector
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.component.AbstractLifecycleComponent
import org.mitallast.queue.common.events.EventBus
import org.mitallast.queue.crdt.bucket.Bucket
import org.mitallast.queue.crdt.event.ClosedLogSynced
import org.mitallast.queue.crdt.protocol.AppendEntries
import org.mitallast.queue.crdt.protocol.AppendRejected
import org.mitallast.queue.crdt.protocol.AppendSuccessful
import org.mitallast.queue.crdt.routing.RoutingReplica
import org.mitallast.queue.crdt.routing.fsm.RoutingTableFSM
import org.mitallast.queue.transport.TransportService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class DefaultReplicator @Inject constructor(
    config: Config,
    private val fsm: RoutingTableFSM,
    private val eventBus: EventBus,
    private val transportService: TransportService,
    @param:Assisted private val bucket: Bucket
) : AbstractLifecycleComponent(), Replicator {

    private val lock = ReentrantLock()
    private val scheduler = Executors.newSingleThreadScheduledExecutor()
    private val replicationIndex = TSynchronizedLongLongMap(TLongLongHashMap(32, 0.5f, 0, 0))
    private val replicationTimeout = TSynchronizedLongLongMap(TLongLongHashMap(32, 0.5f, 0, 0))

    private val timeout = config.getDuration("crdt.timeout", TimeUnit.MILLISECONDS)

    @Volatile private var open = true

    private fun initialize() {
        val routingTable = fsm.get()
        val routingBucket = routingTable.buckets.get(this.bucket.index())
        val replicas = routingBucket.replicas.remove(bucket.replica()).values()

        for (replica in replicas) {
            replicationTimeout.put(replica.id, System.currentTimeMillis() + timeout)
            val appendEntries = AppendEntries(bucket.index(), bucket.replica(), 0, Vector.empty())
            transportService.send(replica.member, appendEntries)
        }
        scheduler.scheduleWithFixedDelay({
            lock.lock()
            try {
                maybeSendEntries()
            } finally {
                lock.unlock()
            }
        }, timeout, timeout, TimeUnit.MILLISECONDS)
    }

    override fun append(id: Long, event: Message) {
        lock.lock()
        try {
            if (!open) {
                throw IllegalStateException("closed")
            }
            bucket.log().append(id, event)
            maybeSendEntries()
        } finally {
            lock.unlock()
        }
    }

    override fun successful(message: AppendSuccessful) {
        lock.lock()
        try {
            if (logger.isDebugEnabled) {
                logger.debug("[replica={}:{}] append successful from={}:{} last={}",
                    bucket.index(), bucket.replica(),
                    message.bucket, message.replica, message.index)
            }
            if (replicationIndex.get(message.replica) < message.index) {
                replicationIndex.put(message.replica, message.index)
            }
            replicationTimeout.put(message.replica, 0)
            maybeSendEntries(message.replica)
            maybeSync()
        } finally {
            lock.unlock()
        }
    }

    override fun rejected(message: AppendRejected) {
        lock.lock()
        try {
            logger.warn("[replica={}:{}] append rejected from={}:{} last={}",
                bucket.index(), bucket.replica(),
                message.bucket, message.replica, message.index)
            if (replicationIndex.get(message.replica) < message.index) {
                replicationIndex.put(message.replica, message.index)
            }
            replicationTimeout.put(message.replica, 0)
            maybeSendEntries(message.replica)
            maybeSync()
        } finally {
            lock.unlock()
        }
    }

    override fun open() {
        lock.lock()
        try {
            open = true
        } finally {
            lock.unlock()
        }
    }

    override fun closeAndSync() {
        lock.lock()
        try {
            open = false
            maybeSync()
        } finally {
            lock.unlock()
        }
    }

    private fun maybeSendEntries() {
        val routingTable = fsm.get()
        val routingBucket = routingTable.buckets.get(this.bucket.index())
        routingBucket.replicas.remove(bucket.replica())
            .values().forEach { this.maybeSendEntries(it) }
    }

    private fun maybeSendEntries(replica: Long) {
        val routingTable = fsm.get()
        val routingBucket = routingTable.buckets.get(bucket.index())
        routingBucket.replicas.get(replica).forEach { this.maybeSendEntries(it) }
    }

    private fun maybeSendEntries(replica: RoutingReplica) {
        if (replica.id == bucket.replica()) { // do not send to self
            return
        }
        val timeout = replicationTimeout.get(replica.id)
        if (timeout == 0L) {
            if (logger.isTraceEnabled) {
                logger.trace("[replica={}:{}] no request in progress at {}:{}",
                    bucket.index(), bucket.replica(),
                    bucket.index(), replica.id)
            }
            sendEntries(replica)
        } else if (timeout < System.currentTimeMillis()) {
            logger.warn("[replica={}:{}] request timeout at {}:{}",
                bucket.index(), bucket.replica(),
                bucket.index(), replica.id)
            sendEntries(replica)
        } else {
            if (logger.isTraceEnabled) {
                logger.trace("[replica={}:{}] request in progress to {}:{}",
                    bucket.index(), bucket.replica(),
                    bucket.index(), replica.id)
            }
        }
    }

    private fun sendEntries(replica: RoutingReplica) {
        val prev = replicationIndex.get(replica.id)
        val log = bucket.log()
        val append = log.entriesFrom(prev).take(10000)
        if (append.nonEmpty()) {
            if (logger.isDebugEnabled) {
                logger.debug("[replica={}:{}] send append to={}:{} prev={} entries: {}",
                    bucket.index(), bucket.replica(),
                    bucket.index(), replica.id, prev, append)
            }
            replicationTimeout.put(replica.id, System.currentTimeMillis() + timeout)
            transportService.send(replica.member, AppendEntries(bucket.index(), bucket.replica(), prev, append))
        } else {
            if (logger.isTraceEnabled) {
                logger.trace("no new entries")
            }
        }
    }

    private fun maybeSync() {
        if (!open) {
            val last = bucket.log().index()

            val routingTable = fsm.get()
            val routingBucket = routingTable.buckets.get(bucket.index())
            val replicas = routingBucket.replicas.remove(bucket.replica()).values()
            if (replicas.isEmpty) { // no replica
                return
            }
            for (replica in replicas) {
                if (replicationIndex.get(replica.id) != last) {
                    return  // not synced
                }
            }
            eventBus.trigger(ClosedLogSynced(bucket.index(), bucket.replica()))
        }
    }

    override fun doStart() {
        lock.lock()
        try {
            initialize()
        } finally {
            lock.unlock()
        }
    }

    override fun doStop() {}

    override fun doClose() {}
}
