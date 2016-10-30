package org.mitallast.queue.blob;

import com.google.inject.AbstractModule;
import org.mitallast.queue.raft.ResourceFSM;

public class BlobModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LocalBlobStorageService.class).asEagerSingleton();
        bind(DistributedStorageService.class).asEagerSingleton();
        bind(DistributedStorageFSM.class).asEagerSingleton();
        bind(BlobStreamService.class).asEagerSingleton();

        bind(BlobStorageService.class).to(LocalBlobStorageService.class);
        bind(ResourceFSM.class).to(DistributedStorageFSM.class);
    }
}
