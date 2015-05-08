package org.mitallast.queue.common.component;

import org.mitallast.queue.QueueException;

public interface LifecycleComponent {

    Lifecycle.State lifecycleState();

    void start() throws QueueException;

    void stop() throws QueueException;

    void close() throws QueueException;
}
