package org.mitallast.queue.blob;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.mitallast.queue.blob.protocol.*;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.raft.resource.ResourceFSM;

public class BlobModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LocalBlobStorageService.class).asEagerSingleton();
        bind(DistributedStorageService.class).asEagerSingleton();
        bind(DistributedStorageFSM.class).asEagerSingleton();

        bind(BlobStorageService.class).to(LocalBlobStorageService.class);

        Multibinder<StreamableRegistry> streamableBinder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(BlobRoutingMap.class, BlobRoutingMap::new, 500));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(PutBlobResource.class, PutBlobResource::new, 501));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(PutBlobResourceRequest.class, PutBlobResourceRequest::new, 502));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(PutBlobResourceResponse.class, PutBlobResourceResponse::new, 503));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(GetBlobResourceRequest.class, GetBlobResourceRequest::new, 504));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(GetBlobResourceResponse.class, GetBlobResourceResponse::new, 505));
    }
}
