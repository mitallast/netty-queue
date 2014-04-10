package org.mitallast.queue.queue;

import org.mitallast.queue.common.module.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

public class AbstractQueueComponent extends AbstractComponent implements QueueComponent {
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
