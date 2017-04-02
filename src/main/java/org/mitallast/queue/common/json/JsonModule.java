package org.mitallast.queue.common.json;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.mitallast.queue.common.stream.StreamableRegistry;

import static org.mitallast.queue.common.stream.StreamableRegistry.of;

public class JsonModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(JsonService.class).asEagerSingleton();

        Multibinder<StreamableRegistry> binder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);

        binder.addBinding().toInstance(of(JsonStreamable.class, JsonStreamable::new, 6000));
    }
}
