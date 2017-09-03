package org.mitallast.queue.raft

import com.google.inject.Inject
import io.netty.util.concurrent.DefaultThreadFactory
import org.mitallast.queue.common.component.AbstractLifecycleComponent
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

interface RaftContext {

    fun setTimer(name: String, delayMs: Long, task: () -> Unit)

    fun startTimer(name: String, delayMs: Long, periodMs: Long, task: () -> Unit)

    fun cancelTimer(name: String)

    companion object {
        val ELECTION_TIMEOUT = "election-timeout"
        val SEND_HEARTBEAT = "send-heartbeat"
    }
}

class DefaultRaftContext @Inject constructor() : AbstractLifecycleComponent(), RaftContext {

    private val timers = ConcurrentHashMap<String, ScheduledFuture<*>>()
    private val scheduler= Executors.newScheduledThreadPool(1, DefaultThreadFactory("raft"))

    override fun doStart() {}

    override fun doStop() {}

    override fun doClose() {
        scheduler.shutdown()
    }

    override fun setTimer(name: String, delayMs: Long, task: () -> Unit) {
        timers.compute(name) { _, prev ->
            prev?.cancel(true)
            scheduler.schedule(task, delayMs, TimeUnit.MILLISECONDS)
        }
    }

    override fun startTimer(name: String, delayMs: Long, periodMs: Long, task: () -> Unit) {
        timers.compute(name) { _, prev ->
            prev?.cancel(true)
            scheduler.scheduleAtFixedRate(task, delayMs, periodMs, TimeUnit.MILLISECONDS)
        }
    }

    override fun cancelTimer(name: String) {
        val prev = timers.remove(name)
        prev?.cancel(true)
    }
}

class TestRaftContext : RaftContext {
    private val timers = ConcurrentHashMap<String, () -> Unit>()

    override fun setTimer(name: String, delayMs: Long, task: () -> Unit) {
        timers.put(name, task)
    }

    override fun startTimer(name: String, delayMs: Long, periodMs: Long, task: () -> Unit) {
        timers.put(name, task)
    }

    override fun cancelTimer(name: String) {
        timers.remove(name)
    }

    fun runTimer(name: String) {
        val runnable = timers[name]
        runnable!!.invoke()
    }
}