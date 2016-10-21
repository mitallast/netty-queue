package org.mitallast.queue.common.component;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractComponent {
    protected final Logger logger;
    protected final Config config;

    public AbstractComponent(Config config, Class loggerClass) {
        this.config = config;
        this.logger = LoggerFactory.getLogger(loggerClass);
    }
}
