package org.mitallast.queue.transport

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import javaslang.collection.HashMap
import org.junit.Before
import org.junit.Test
import org.mitallast.queue.common.BaseIntegrationTest
import org.mitallast.queue.common.BaseQueueTest
import java.util.concurrent.CountDownLatch

class TransportBenchmark : BaseQueueTest() {

    private var transportService: TransportService? = null
    private var member: DiscoveryNode = DiscoveryNode("", 0)
    private var countDownLatch: CountDownLatch? = null

    override fun max(): Int {
        return 2000
    }

    @Throws(Exception::class)
    override fun config(): Config {
        val config = HashMap.of<String, Any>(
            "rest.enabled", false,
            "raft.enabled", false,
            "blob.enabled", false
        )
        return ConfigFactory.parseMap(config.toJavaMap()).withFallback(super.config())
    }

    @Before
    @Throws(Exception::class)
    fun setUp() {
        transportService = node().injector().getInstance(TransportService::class.java)
        val transportServer = node().injector().getInstance(TransportServer::class.java)

        val transportController = node().injector().getInstance(TransportController::class.java)
        transportController.registerMessageHandler(BaseIntegrationTest.TestStreamable::class.java) { _ ->
            countDownLatch!!.countDown()
        }

        member = transportServer.localNode()
        transportService!!.connectToNode(member)
    }

    @Test
    @Throws(Exception::class)
    fun test() {
        for (e in 0..9) {
            System.gc()
            countDownLatch = CountDownLatch(total())
            val start = System.currentTimeMillis()
            for (i in 0 until total()) {
                transportService!!.send(member, BaseIntegrationTest.TestStreamable(i.toLong()))
            }
            countDownLatch!!.await()
            val end = System.currentTimeMillis()
            printQps("send", total().toLong(), start, end)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testConcurrent() {
        for (e in 0..9) {
            countDownLatch = CountDownLatch(total())
            val start = System.currentTimeMillis()
            executeConcurrent { thread, concurrency ->
                var i = thread
                while (i < total()) {
                    transportService!!.send(member, BaseIntegrationTest.TestStreamable(i.toLong()))
                    i += concurrency
                }
            }
            countDownLatch!!.await()
            val end = System.currentTimeMillis()
            printQps("send concurrent", total().toLong(), start, end)
        }
    }
}
