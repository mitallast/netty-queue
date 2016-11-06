package org.mitallast.queue.common.stream;

import com.google.inject.AbstractModule;

public class StreamModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(StreamService.class).to(InternalStreamService.class);
        bind(InternalStreamService.class).asEagerSingleton();

        // Multibinder<StreamableRegistry> streamableBinder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);
        // streamableBinder.addBinding()
    }
}
