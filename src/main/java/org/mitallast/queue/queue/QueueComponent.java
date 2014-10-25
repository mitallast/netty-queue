package org.mitallast.queue.queue;

import org.mitallast.queue.common.component.LifecycleComponent;
import org.mitallast.queue.common.settings.Settings;

public interface QueueComponent extends LifecycleComponent {

    Queue queue();

    Settings queueSettings();
}
