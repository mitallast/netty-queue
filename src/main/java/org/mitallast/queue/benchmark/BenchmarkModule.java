package org.mitallast.queue.benchmark;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.raft.ResourceFSM;

public class BenchmarkModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(BenchmarkService.class).asEagerSingleton();
        bind(BenchmarkFSM.class).asEagerSingleton();
        bind(ResourceFSM.class).to(BenchmarkFSM.class);

        Multibinder<StreamableRegistry> streamableBinder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(BenchmarkRequest.class, BenchmarkRequest::new, 5501));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(BenchmarkResponse.class, BenchmarkResponse::new, 5502));
    }
}
