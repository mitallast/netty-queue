package org.mitallast.queue.transport

import org.mitallast.queue.common.codec.Message

interface TransportService {

    fun connectToNode(node: DiscoveryNode)

    fun disconnectFromNode(node: DiscoveryNode)

    fun send(node: DiscoveryNode, message: Message)
}
