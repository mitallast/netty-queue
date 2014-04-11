package org.mitallast.queue.queue;

import org.mitallast.queue.common.component.LifecycleComponent;

public interface QueueComponent extends LifecycleComponent {
    Queue queue();
}
