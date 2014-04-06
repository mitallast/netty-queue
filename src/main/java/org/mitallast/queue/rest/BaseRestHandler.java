package org.mitallast.queue.rest;

import org.mitallast.queue.client.Service;
import org.mitallast.queue.common.module.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

public abstract class BaseRestHandler extends AbstractComponent implements RestHandler {
    protected final Service service;

    public BaseRestHandler(Settings settings, Service service) {
        super(settings);
        this.service = service;
    }
}
