package org.mitallast.queue.crdt.bucket

import com.google.inject.assistedinject.Assisted
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.crdt.log.DefaultCompactionFilter
import org.mitallast.queue.crdt.log.ReplicatedLog
import org.mitallast.queue.crdt.log.ReplicatedLogFactory
import org.mitallast.queue.crdt.registry.CrdtRegistry
import org.mitallast.queue.crdt.registry.CrdtRegistryFactory
import org.mitallast.queue.crdt.replication.Replicator
import org.mitallast.queue.crdt.replication.ReplicatorFactory
import org.mitallast.queue.crdt.replication.state.ReplicaState
import org.mitallast.queue.crdt.replication.state.ReplicaStateFactory
import java.util.concurrent.locks.ReentrantLock
import javax.inject.Inject

class DefaultBucket @Inject
constructor(
    @param:Assisted private val index: Int,
    @param:Assisted private val replica: Long,
    crdtRegistryFactory: CrdtRegistryFactory,
    logFactory: ReplicatedLogFactory,
    stateFactory: ReplicaStateFactory,
    replicatorFactory: ReplicatorFactory
) : Bucket {
    private val logger = LogManager.getLogger("replicator[$index]")
    private val lock = ReentrantLock()
    private val replicaState = stateFactory.create(index, replica)
    private val replicator = replicatorFactory.create(this)
    private val registry = crdtRegistryFactory.create(index, replica, replicator)
    private val log = logFactory.create(index, replica, DefaultCompactionFilter(registry))

    init {
        replicator.start()
    }

    override fun index(): Int {
        return index
    }

    override fun replica(): Long {
        return replica
    }

    override fun lock(): ReentrantLock {
        return lock
    }

    override fun registry(): CrdtRegistry {
        return registry
    }

    override fun log(): ReplicatedLog {
        return log
    }

    override fun replicator(): Replicator {
        return replicator
    }

    override fun state(): ReplicaState {
        return replicaState
    }

    override fun close() {
        logger.info("close")
        replicator.stop()
        log.close()
        replicaState.close()
    }

    override fun delete() {
        logger.info("delete")
        log.delete()
        replicaState.delete()
    }
}
