package org.mitallast.queue.common;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class ConfigBuilder {
    private Config config = ConfigFactory.empty();

    public ConfigBuilder with(String path, Object value) {
        config = config.withValue(path, ConfigValueFactory.fromAnyRef(value));
        return this;
    }

    public Config build() {
        return config.withFallback(ConfigFactory.defaultReference());
    }
}
