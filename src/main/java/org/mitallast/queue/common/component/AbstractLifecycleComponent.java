package org.mitallast.queue.common.component;

import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.settings.Settings;

public abstract class AbstractLifecycleComponent extends AbstractComponent implements LifecycleComponent {

    protected final Lifecycle lifecycle = new Lifecycle();

    protected AbstractLifecycleComponent(Settings settings) {
        super(settings);
    }

    protected AbstractLifecycleComponent(Settings settings, Class customClass) {
        super(settings, customClass);
    }

    protected AbstractLifecycleComponent(Settings settings, Class loggerClass, Class componentClass) {
        super(settings, loggerClass, componentClass);
    }

    protected AbstractLifecycleComponent(Settings settings, String prefixSettings) {
        super(settings, prefixSettings);
    }

    protected AbstractLifecycleComponent(Settings settings, String prefixSettings, Class customClass) {
        super(settings, prefixSettings, customClass);
    }

    protected AbstractLifecycleComponent(Settings settings, String prefixSettings, Class loggerClass, Class componentClass) {
        super(settings, prefixSettings, loggerClass, componentClass);
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void start() throws QueueException {
        if (!lifecycle.canMoveToStarted()) {
            return;
        }
        doStart();
        lifecycle.moveToStarted();
    }

    protected abstract void doStart() throws QueueException;

    @SuppressWarnings({"unchecked"})
    @Override
    public void stop() throws QueueException {
        if (!lifecycle.canMoveToStopped()) {
            return;
        }
        lifecycle.moveToStopped();
        doStop();
    }

    protected abstract void doStop() throws QueueException;

    @Override
    public void close() throws QueueException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.canMoveToClosed()) {
            return;
        }
        lifecycle.moveToClosed();
        doClose();
    }

    protected abstract void doClose() throws QueueException;
}

