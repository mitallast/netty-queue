package org.mitallast.queue.common.path

import org.junit.Test
import org.mitallast.queue.common.BaseTest
import java.util.*

class PathTrie2Benchmark : BaseTest() {
    private val params = HashMap<String, String>()
    private val pathTrie = TrieNode.node<String>()
            .insert("/", "1")
            .insert("/_stats", "2")
            .insert("/{queue}", "3")
            .insert("/{queue}/_stats", "4")
            .insert("/{queue}/message", "5")
            .insert("/{queue}/message/{uuid}", "6")

    @Test
    @Throws(Exception::class)
    fun testRetrieveRoot() {
        params.clear()
        val start = System.currentTimeMillis()
        for (i in 0..9999999) {
            pathTrie.retrieve("/")
        }
        val end = System.currentTimeMillis()
        printQps("retrieve /", 10000000, start, end)
    }

    @Test
    @Throws(Exception::class)
    fun testRetrieveStats() {
        params.clear()
        val start = System.currentTimeMillis()
        for (i in 0..9999999) {
            pathTrie.retrieve("/_stats")
        }
        val end = System.currentTimeMillis()
        printQps("retrieve /_stats", 10000000, start, end)
    }

    @Test
    @Throws(Exception::class)
    fun testRetrieveQueue() {
        params.clear()
        val start = System.currentTimeMillis()
        for (i in 0..9999999) {
            pathTrie.retrieve("/queue")
        }
        val end = System.currentTimeMillis()
        printQps("retrieve /queue", 10000000, start, end)
    }

    @Test
    @Throws(Exception::class)
    fun testRetrieveQueueStats() {
        params.clear()
        val start = System.currentTimeMillis()
        for (i in 0..9999999) {
            pathTrie.retrieve("/queue/_stats")
        }
        val end = System.currentTimeMillis()
        printQps("retrieve /queue/_stats", 10000000, start, end)
    }

    @Test
    @Throws(Exception::class)
    fun testRetrieveQueueMessage() {
        params.clear()
        val start = System.currentTimeMillis()
        for (i in 0..9999999) {
            pathTrie.retrieve("/queue/message")
        }
        val end = System.currentTimeMillis()
        printQps("retrieve /queue/message", 10000000, start, end)
    }

    @Test
    @Throws(Exception::class)
    fun testRetrieveQueueMessageUuid() {
        params.clear()
        val start = System.currentTimeMillis()
        for (i in 0..9999999) {
            pathTrie.retrieve("/queue/message/uuid")
        }
        val end = System.currentTimeMillis()
        printQps("retrieve /queue/message/uuid", 10000000, start, end)
    }
}
