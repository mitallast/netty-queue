package org.mitallast.queue

import com.typesafe.config.ConfigFactory
import org.mitallast.queue.node.InternalNode
import java.util.concurrent.CountDownLatch

object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        val config = ConfigFactory.load()
        val node = InternalNode(config)
        node.start()

        val countDownLatch = CountDownLatch(1)
        Runtime.getRuntime().addShutdownHook(Thread {
            node.close()
            countDownLatch.countDown()
        })
        countDownLatch.await()
    }
}
