package org.mitallast.queue.common.module;

import org.mitallast.queue.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractComponent {
    protected final Logger logger;
    protected final Settings settings;
    protected Settings componentSettings;

    public AbstractComponent(Settings settings) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.settings = settings;
        this.componentSettings = settings.getComponentSettings(getClass());
    }

    public AbstractComponent(Settings settings, String prefixSettings) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.settings = settings;
        this.componentSettings = settings.getComponentSettings(prefixSettings, getClass());
    }

    public AbstractComponent(Settings settings, Class customClass) {
        this.logger = LoggerFactory.getLogger(customClass);
        this.settings = settings;
        this.componentSettings = settings.getComponentSettings(customClass);
    }

    public AbstractComponent(Settings settings, String prefixSettings, Class customClass) {
        this.logger = LoggerFactory.getLogger(customClass);
        this.settings = settings;
        this.componentSettings = settings.getComponentSettings(prefixSettings, customClass);
    }

    public AbstractComponent(Settings settings, Class loggerClass, Class componentClass) {
        this.logger = LoggerFactory.getLogger(loggerClass);
        this.settings = settings;
        this.componentSettings = settings.getComponentSettings(componentClass);
    }

    public AbstractComponent(Settings settings, String prefixSettings, Class loggerClass, Class componentClass) {
        this.logger = LoggerFactory.getLogger(loggerClass);
        this.settings = settings;
        this.componentSettings = settings.getComponentSettings(prefixSettings, componentClass);
    }
}
