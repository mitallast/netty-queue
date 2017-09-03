package org.mitallast.queue.transport

interface TransportServer {

    fun localNode(): DiscoveryNode

    companion object {
        val DEFAULT_PORT = 8900
    }
}
