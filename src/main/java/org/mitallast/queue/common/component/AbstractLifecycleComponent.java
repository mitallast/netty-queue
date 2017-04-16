package org.mitallast.queue.common.component;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractLifecycleComponent implements LifecycleComponent {
    protected final Logger logger = LogManager.getLogger(getClass());
    private final Lifecycle lifecycle = new Lifecycle();

    @Override
    public Lifecycle lifecycle() {
        return lifecycle;
    }

    @Override
    public void checkIsStarted() {
        if (!lifecycle.started()) {
            throw new IllegalStateException(getClass().getSimpleName() + " is not started");
        }
    }

    @Override
    public void checkIsStopped() {
        if (!lifecycle.stopped()) {
            throw new IllegalStateException(getClass().getSimpleName() + " is not stopped");
        }
    }

    @Override
    public void checkIsClosed() {
        if (!lifecycle.closed()) {
            throw new IllegalStateException(getClass().getSimpleName() + " is not closed");
        }
    }

    @Override
    public void start() {
        if (!lifecycle.canMoveToStarted()) {
            logger.warn("Can't move to started, " + lifecycle.state());
            return;
        }
        if (!lifecycle.moveToStarted()) {
            logger.warn("Don't moved to started, " + lifecycle.state());
        }
        logger.info("starting");
        doStart();
        logger.info("started");
    }

    protected abstract void doStart();

    @Override
    public void stop() {
        if (!lifecycle.canMoveToStopped()) {
            logger.warn("Can't move to stopped, it's a " + lifecycle.state());
            return;
        }
        lifecycle.moveToStopped();
        logger.info("stopping");
        doStop();
        logger.debug("stopped");
    }

    protected abstract void doStop();

    @Override
    public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.canMoveToClosed()) {
            logger.warn("Can't move to closed, it's a " + lifecycle.state());
            return;
        }
        lifecycle.moveToClosed();
        logger.info("closing");
        doClose();
        logger.info("closed");
    }

    protected abstract void doClose();
}

