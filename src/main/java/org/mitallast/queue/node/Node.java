package org.mitallast.queue.node;

import com.google.inject.Injector;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.cluster.DiscoveryNode;
import org.mitallast.queue.common.component.LifecycleComponent;
import org.mitallast.queue.common.settings.Settings;

public interface Node extends LifecycleComponent {

    Settings settings();

    Injector injector();

    Client localClient();

    DiscoveryNode localNode();
}
