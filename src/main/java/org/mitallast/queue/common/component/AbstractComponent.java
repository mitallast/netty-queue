package org.mitallast.queue.common.component;

import org.mitallast.queue.common.logging.Loggers;
import org.mitallast.queue.common.settings.Settings;
import org.slf4j.Logger;

public abstract class AbstractComponent {
    protected final Logger logger;
    protected final Settings settings;
    protected final Settings componentSettings;

    public AbstractComponent(Settings settings) {
        this.settings = settings;
        this.logger = Loggers.getLogger(getClass(), settings);
        this.componentSettings = settings.getComponentSettings(getClass());
    }

    public AbstractComponent(Settings settings, String prefixSettings) {
        this.settings = settings;
        this.logger = Loggers.getLogger(getClass(), settings);
        this.componentSettings = settings.getComponentSettings(prefixSettings, getClass());
    }

    public AbstractComponent(Settings settings, Class customClass) {
        this.settings = settings;
        this.logger = Loggers.getLogger(customClass, settings);
        this.componentSettings = settings.getComponentSettings(customClass);
    }

    public AbstractComponent(Settings settings, String prefixSettings, Class customClass) {
        this.settings = settings;
        this.logger = Loggers.getLogger(customClass, settings);
        this.componentSettings = settings.getComponentSettings(prefixSettings, customClass);
    }

    public AbstractComponent(Settings settings, Class loggerClass, Class componentClass) {
        this.settings = settings;
        this.logger = Loggers.getLogger(loggerClass, settings);
        this.componentSettings = settings.getComponentSettings(componentClass);
    }

    public AbstractComponent(Settings settings, String prefixSettings, Class loggerClass, Class componentClass) {
        this.settings = settings;
        this.logger = Loggers.getLogger(loggerClass, settings);
        this.componentSettings = settings.getComponentSettings(prefixSettings, componentClass);
    }
}
