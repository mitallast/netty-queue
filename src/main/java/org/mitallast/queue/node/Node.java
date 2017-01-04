package org.mitallast.queue.node;

import com.google.inject.Injector;
import com.typesafe.config.Config;
import org.mitallast.queue.common.component.LifecycleComponent;
import org.mitallast.queue.transport.DiscoveryNode;

public interface Node extends LifecycleComponent {

    Config config();

    Injector injector();
}
