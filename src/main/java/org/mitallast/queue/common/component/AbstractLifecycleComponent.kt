package org.mitallast.queue.common.component

import org.mitallast.queue.common.logging.LoggingService

abstract class AbstractLifecycleComponent(logging: LoggingService) : LifecycleComponent {
    protected val logger = logging.logger(javaClass)

    private val lifecycle = Lifecycle()

    override fun lifecycle(): Lifecycle {
        return lifecycle
    }

    override fun checkIsStarted() {
        if (!lifecycle.started()) {
            throw IllegalStateException(javaClass.simpleName + " is not started")
        }
    }

    override fun checkIsStopped() {
        if (!lifecycle.stopped()) {
            throw IllegalStateException(javaClass.simpleName + " is not stopped")
        }
    }

    override fun checkIsClosed() {
        if (!lifecycle.closed()) {
            throw IllegalStateException(javaClass.simpleName + " is not closed")
        }
    }

    override fun start() {
        if (!lifecycle.canMoveToStarted()) {
            logger.warn("Can't move to started, " + lifecycle.state())
            return
        }
        if (!lifecycle.moveToStarted()) {
            logger.warn("Don't moved to started, " + lifecycle.state())
        }
        logger.info("starting")
        doStart()
        logger.info("started")
    }

    protected abstract fun doStart()

    override fun stop() {
        if (!lifecycle.canMoveToStopped()) {
            logger.warn("Can't move to stopped, it's a " + lifecycle.state())
            return
        }
        lifecycle.moveToStopped()
        logger.info("stopping")
        doStop()
        logger.debug("stopped")
    }

    protected abstract fun doStop()

    override fun close() {
        if (lifecycle.started()) {
            stop()
        }
        if (!lifecycle.canMoveToClosed()) {
            logger.warn("Can't move to closed, it's a " + lifecycle.state())
            return
        }
        lifecycle.moveToClosed()
        logger.info("closing")
        doClose()
        logger.info("closed")
    }

    protected abstract fun doClose()
}

