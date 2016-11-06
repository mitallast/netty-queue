package org.mitallast.queue.blob;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.mitallast.queue.blob.protocol.*;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.raft.ResourceFSM;

public class BlobModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LocalBlobStorageService.class).asEagerSingleton();
        bind(DistributedStorageService.class).asEagerSingleton();
        bind(DistributedStorageFSM.class).asEagerSingleton();

        bind(BlobStorageService.class).to(LocalBlobStorageService.class);
        bind(ResourceFSM.class).to(DistributedStorageFSM.class);

        Multibinder<StreamableRegistry> streamableBinder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(BlobRoutingMap.class, BlobRoutingMap::new, 300));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(PutBlobResource.class, PutBlobResource::new, 301));

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(PutBlobResourceRequest.class, PutBlobResourceRequest::new, 302));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(PutBlobResourceResponse.class, PutBlobResourceResponse::new, 303));

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(GetBlobResourceRequest.class, GetBlobResourceRequest::new, 304));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(GetBlobResourceResponse.class, GetBlobResourceResponse::new, 305));
    }
}
