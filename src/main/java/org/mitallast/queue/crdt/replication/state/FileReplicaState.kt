package org.mitallast.queue.crdt.replication.state

import com.google.inject.Inject
import com.google.inject.assistedinject.Assisted
import gnu.trove.impl.sync.TSynchronizedLongLongMap
import gnu.trove.map.TLongLongMap
import gnu.trove.map.hash.TLongLongHashMap
import org.mitallast.queue.common.file.FileService
import java.io.DataOutputStream
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

class FileReplicaState @Inject
constructor(
    private val fileService: FileService,
    @Assisted index: Int,
    @Assisted replicaId: Long
) : ReplicaState {
    private val indexMap: TLongLongMap
    private val size: AtomicInteger
    private val writeLock: ReentrantLock
    private val serviceName: String

    @Volatile private var file: File
    @Volatile private var output: DataOutputStream

    init {
        this.indexMap = TSynchronizedLongLongMap(TLongLongHashMap(7, 0.5f, 0, 0))
        this.size = AtomicInteger()
        this.writeLock = ReentrantLock()
        this.serviceName = String.format("crdt/%d/replica/%d", index, replicaId)

        this.file = fileService.resource(serviceName, "state.log")
        this.output = fileService.output(file, true)

        if (file.length() > 0) {
            fileService.input(file).use { stream ->
                while (stream.available() > 0) {
                    indexMap.put(stream.readLong(), stream.readLong())
                    size.incrementAndGet()
                }
            }
        }
    }

    override fun put(replica: Long, logIndex: Long) {
        writeLock.lock()
        try {
            assert(indexMap.get(replica) <= logIndex)
            indexMap.put(replica, logIndex)
            output.writeLong(replica)
            output.writeLong(logIndex)
            val size = this.size.incrementAndGet()
            if (size > indexMap.size() + 1000000) {
                output.close()

                val tmp = fileService.temporary(serviceName, "state", "log")
                fileService.output(tmp).use { stream ->
                    val iterator = indexMap.iterator()
                    while (iterator.hasNext()) {
                        iterator.advance()
                        stream.writeLong(iterator.key())
                        stream.writeLong(iterator.value())
                    }
                }

                fileService.move(tmp, file)

                this.file = fileService.resource(serviceName, "state.log")
                this.output = fileService.output(file, true)
            }
        } finally {
            writeLock.unlock()
        }
    }

    override fun get(replica: Long): Long {
        return indexMap.get(replica)
    }

    override fun close() {
        writeLock.lock()
        try {
            output.close()
        } finally {
            writeLock.unlock()
        }
    }

    override fun delete() {
        close()
        fileService.delete(serviceName)
    }
}
