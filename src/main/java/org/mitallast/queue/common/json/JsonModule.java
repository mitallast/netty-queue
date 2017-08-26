package org.mitallast.queue.common.json;

import com.google.inject.AbstractModule;
import org.mitallast.queue.common.codec.Codec;

public class JsonModule extends AbstractModule {
    static {
        Codec.register(6000, JsonMessage.class, JsonMessage.codec);
    }

    @Override
    protected void configure() {
        bind(JsonService.class).asEagerSingleton();
    }
}
