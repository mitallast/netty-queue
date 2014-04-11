package org.mitallast.queue.queue;

import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;

public abstract class AbstractQueueComponent extends AbstractLifecycleComponent implements QueueComponent {

    protected final Queue queue;
    protected final Settings queueSettings;

    public AbstractQueueComponent(Settings settings, Settings queueSettings, Queue queue) {
        super(settings);
        this.queue = queue;
        this.queueSettings = queueSettings;
    }

    @Override
    public Queue queue() {
        return queue;
    }
}
