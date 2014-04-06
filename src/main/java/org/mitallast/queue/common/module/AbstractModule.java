package org.mitallast.queue.common.module;

import org.mitallast.queue.common.settings.Settings;

public abstract class AbstractModule extends AbstractComponent implements Module {
    public AbstractModule(Settings settings) {
        super(settings);
    }
}
