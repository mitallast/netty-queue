package org.mitallast.queue.crdt;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.crdt.commutative.CmRDTService;
import org.mitallast.queue.crdt.commutative.LWWRegister;

import static org.mitallast.queue.common.stream.StreamableRegistry.of;

public class CrdtModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(CmRDTService.class).asEagerSingleton();

        Multibinder<StreamableRegistry> binder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);

        binder.addBinding().toInstance(of(LWWRegister.SourceAssign.class, LWWRegister.SourceAssign::new, 1000));
        binder.addBinding().toInstance(of(LWWRegister.DownstreamAssign.class, LWWRegister.DownstreamAssign::new, 1001));
        binder.addBinding().toInstance(of(LWWRegister.Value.class, LWWRegister.Value::read, 1002));
        binder.addBinding().toInstance(of(LWWRegister.ValueResponse.class, LWWRegister.ValueResponse::new, 1003));

        binder.addBinding().toInstance(of(LongStreamable.class, LongStreamable::new, 1100));
    }
}
