package org.mitallast.queue.common.component;

import java.io.IOException;

public interface LifecycleComponent {

    Lifecycle lifecycle();

    void checkIsStarted() throws IllegalStateException;

    void checkIsStopped() throws IllegalStateException;

    void checkIsClosed() throws IllegalStateException;

    void start() throws IOException;

    void stop() throws IOException;

    void close() throws IOException;
}
