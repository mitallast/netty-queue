package org.mitallast.queue.raft.resource;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.mitallast.queue.raft.StateMachine;

public class ResourceFactory {
    private final Injector injector;

    @Inject
    public ResourceFactory(Injector injector) {
        this.injector = injector;
    }

    public <T extends StateMachine> T create(final Class<T> resourceClass, final String path) {
        return injector.createChildInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(String.class).annotatedWith(Names.named("path")).toInstance(path);
                bind(resourceClass);
            }
        }).getInstance(resourceClass);
    }
}
