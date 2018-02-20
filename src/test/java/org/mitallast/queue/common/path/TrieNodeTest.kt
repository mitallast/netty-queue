package org.mitallast.queue.common.path

import io.vavr.control.Option
import org.junit.Assert
import org.junit.Test

class PathTrie2Test {

    @Test
    fun testRoot() {
        var pathTrie = TrieNode.node<String>()

        pathTrie = pathTrie.insert("/", "root")
        pathTrie = pathTrie.insert("/foo", "foo")
        pathTrie.prettyPrint()

        Assert.assertEquals(Option.some("root"), pathTrie.retrieve("/").first)
    }

    @Test
    fun testNamed() {
        var pathTrie = TrieNode.node<String>()

        pathTrie = pathTrie.insert("/", "root")
        pathTrie = pathTrie.insert("/{named}", "{named}")
        pathTrie.prettyPrint()

        val (res, params) = pathTrie.retrieve("/foo")
        Assert.assertEquals(Option.some("{named}"), res)
        Assert.assertTrue(params.containsKey("named"))
        Assert.assertNotNull(params["named"])
        Assert.assertEquals(Option.some("foo"), params["named"])
    }

    @Test
    fun testNamedAndAll() {
        var pathTrie = TrieNode.node<String>()

        pathTrie = pathTrie.insert("/", "root")
        pathTrie = pathTrie.insert("/_all", "_all")
        pathTrie = pathTrie.insert("/{named}", "{named}")
        pathTrie.prettyPrint()

        val (res, params) = pathTrie.retrieve("/foo")
        Assert.assertEquals(Option.some("{named}"), res)
        Assert.assertTrue(params.containsKey("named"))
        Assert.assertNotNull(params["named"])
        Assert.assertEquals(Option.some("foo"), params["named"])
    }

    @Test
    fun testAllAndNamed() {
        var pathTrie = TrieNode.node<String>()

        pathTrie = pathTrie.insert("/", "root")
        pathTrie = pathTrie.insert("/_all", "_all")
        pathTrie = pathTrie.insert("/{named}", "{named}")
        pathTrie.prettyPrint()

        Assert.assertEquals(Option.some("_all"), pathTrie.retrieve("/_all").first)
    }

    @Test
    fun testLevel() {
        var pathTrie = TrieNode.node<String>()

        pathTrie = pathTrie.insert("/", "root")
        pathTrie = pathTrie.insert("/foo", "one")
        pathTrie = pathTrie.insert("/foo/foo", "two")
        pathTrie = pathTrie.insert("/foo/foo/foo", "three")
        pathTrie.prettyPrint()

        Assert.assertEquals(Option.some("root"), pathTrie.retrieve("/").first)
        Assert.assertEquals(Option.some("one"), pathTrie.retrieve("/foo").first)
        Assert.assertEquals(Option.some("two"), pathTrie.retrieve("/foo/foo").first)
        Assert.assertEquals(Option.some("three"), pathTrie.retrieve("/foo/foo/foo").first)
    }

    @Test
    fun testRouteRoot() {
        var pathTrie = TrieNode.node<String>()

        pathTrie = pathTrie.insert("/", "1")
        pathTrie = pathTrie.insert("/_stats", "2")
        pathTrie = pathTrie.insert("/{queue}", "3")
        pathTrie = pathTrie.insert("/{queue}/_stats", "4")
        pathTrie = pathTrie.insert("/{queue}/message", "5")
        pathTrie = pathTrie.insert("/{queue}/message/{uuid}", "6")
        pathTrie.prettyPrint()

        Assert.assertEquals(Option.some("1"), pathTrie.retrieve("/").first)
    }

    @Test
    fun testRouteStats() {
        var pathTrie = TrieNode.node<String>()

        pathTrie = pathTrie.insert("/", "1")
        pathTrie = pathTrie.insert("/_stats", "2")
        pathTrie = pathTrie.insert("/{queue}", "3")
        pathTrie = pathTrie.insert("/{queue}/_stats", "4")
        pathTrie = pathTrie.insert("/{queue}/message", "5")
        pathTrie = pathTrie.insert("/{queue}/message/{uuid}", "6")
        pathTrie.prettyPrint()

        Assert.assertEquals(Option.some("2"), pathTrie.retrieve("/_stats").first)
    }

    @Test
    fun testRouteQueue() {
        var pathTrie = TrieNode.node<String>()

        pathTrie = pathTrie.insert("/", "1")
        pathTrie = pathTrie.insert("/_stats", "2")
        pathTrie = pathTrie.insert("/{queue}", "3")
        pathTrie = pathTrie.insert("/{queue}/_stats", "4")
        pathTrie = pathTrie.insert("/{queue}/message", "5")
        pathTrie = pathTrie.insert("/{queue}/message/{uuid}", "6")
        pathTrie.prettyPrint()

        val (res, params) = pathTrie.retrieve("/queue")
        Assert.assertEquals(Option.some("3"), res)
        Assert.assertFalse(params.isEmpty)
        Assert.assertEquals(Option.some("queue"), params["queue"])
    }

    @Test
    fun testRouteQueueStats() {
        var pathTrie = TrieNode.node<String>()

        pathTrie = pathTrie.insert("/", "1")
        pathTrie = pathTrie.insert("/_stats", "2")
        pathTrie = pathTrie.insert("/{queue}", "3")
        pathTrie = pathTrie.insert("/{queue}/_stats", "4")
        pathTrie = pathTrie.insert("/{queue}/message", "5")
        pathTrie = pathTrie.insert("/{queue}/message/{uuid}", "6")
        pathTrie.prettyPrint()

        val (res, params) = pathTrie.retrieve("/queue/_stats")
        Assert.assertEquals(Option.some("4"), res)
        Assert.assertFalse(params.isEmpty)
        Assert.assertEquals(Option.some("queue"), params["queue"])
    }

    @Test
    fun testRouteQueueMessage() {
        var pathTrie = TrieNode.node<String>()

        pathTrie = pathTrie.insert("/", "1")
        pathTrie = pathTrie.insert("/_stats", "2")
        pathTrie = pathTrie.insert("/{queue}", "3")
        pathTrie = pathTrie.insert("/{queue}/_stats", "4")
        pathTrie = pathTrie.insert("/{queue}/message", "5")
        pathTrie = pathTrie.insert("/{queue}/message/{uuid}", "6")
        pathTrie.prettyPrint()

        val (res, params) = pathTrie.retrieve("/queue/message")
        Assert.assertEquals(Option.some("5"), res)
        Assert.assertFalse(params.isEmpty)
        Assert.assertEquals(Option.some("queue"), params["queue"])
    }

    @Test
    fun testRouteQueueMessageUuid() {
        var pathTrie = TrieNode.node<String>()

        println("testRouteQueueMessageUuid")
        pathTrie = pathTrie.insert("/", "1")
        pathTrie.prettyPrint()
        println("1--------")
        println()

        println()
        pathTrie = pathTrie.insert("/_stats", "2")
        println("2--------")
        pathTrie.prettyPrint()

        println()
        pathTrie = pathTrie.insert("/{queue}", "3")
        println("3--------")
        pathTrie.prettyPrint()

        println()
        pathTrie = pathTrie.insert("/{queue}/_stats", "4")
        println("4--------")
        pathTrie.prettyPrint()

        println()
        pathTrie = pathTrie.insert("/{queue}/message", "5")
        println("5--------")
        pathTrie.prettyPrint()

        println()
        pathTrie = pathTrie.insert("/{queue}/message/{uuid}", "6")
        println("6--------")
        pathTrie.prettyPrint()


        println()

        pathTrie.prettyPrint()
        println()


        val (res, params) = pathTrie.retrieve("/queue/message/uuid")
        Assert.assertEquals(Option.some("6"), res)
        Assert.assertFalse(params.isEmpty)
        Assert.assertEquals(Option.some("queue"), params["queue"])
        Assert.assertEquals(Option.some("uuid"), params["uuid"])
    }
}
