package org.mitallast.queue.common.component

interface LifecycleComponent {

    fun lifecycle(): Lifecycle

    @Throws(IllegalStateException::class)
    fun checkIsStarted()

    @Throws(IllegalStateException::class)
    fun checkIsStopped()

    @Throws(IllegalStateException::class)
    fun checkIsClosed()

    fun start()

    fun stop()

    fun close()
}
