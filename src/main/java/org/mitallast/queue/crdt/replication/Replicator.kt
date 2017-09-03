package org.mitallast.queue.crdt.replication

import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.component.LifecycleComponent
import org.mitallast.queue.crdt.protocol.AppendRejected
import org.mitallast.queue.crdt.protocol.AppendSuccessful

interface Replicator : LifecycleComponent {

    fun append(id: Long, event: Message)

    fun successful(message: AppendSuccessful)

    fun rejected(message: AppendRejected)

    fun open()

    fun closeAndSync()
}
