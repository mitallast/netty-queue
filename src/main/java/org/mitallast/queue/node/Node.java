package org.mitallast.queue.node;

import com.google.inject.Injector;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.client.local.LocalClient;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.client.TransportClient;

public interface Node {
    /**
     * The settings that were used to create the node.
     */
    Settings settings();

    /**
     * Start the node. If the node is already started, this method is no-op.
     */
    Node start();

    /**
     * Stops the node. If the node is already stopped, this method is no-op.
     */
    Node stop();

    /**
     * Closes the node (and {@link #stop}s if its running).
     */
    void close();

    /**
     * Returns <tt>true</tt> if the node is closed.
     */
    boolean isClosed();

    Injector injector();

    Client client();

    TransportClient transportClient();

    LocalClient localClient();
}
