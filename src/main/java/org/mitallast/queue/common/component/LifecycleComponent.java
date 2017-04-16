package org.mitallast.queue.common.component;

public interface LifecycleComponent {

    Lifecycle lifecycle();

    void checkIsStarted() throws IllegalStateException;

    void checkIsStopped() throws IllegalStateException;

    void checkIsClosed() throws IllegalStateException;

    void start();

    void stop();

    void close();
}
