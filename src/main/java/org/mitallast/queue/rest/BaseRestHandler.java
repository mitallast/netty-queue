package org.mitallast.queue.rest;

import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

public abstract class BaseRestHandler extends AbstractComponent implements RestHandler {
    protected final Client client;

    public BaseRestHandler(Settings settings, Client client) {
        super(settings);
        this.client = client;
    }
}
