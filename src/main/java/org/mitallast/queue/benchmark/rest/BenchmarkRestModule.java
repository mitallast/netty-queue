package org.mitallast.queue.benchmark.rest;

import com.google.inject.AbstractModule;

public class BenchmarkRestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(BenchmarkAction.class).asEagerSingleton();
    }
}
