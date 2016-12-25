package org.mitallast.queue.blob.rest;

import com.google.inject.AbstractModule;

public class BlobRestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(PutBlobResourceAction.class).asEagerSingleton();
        bind(GetBlobResourceAction.class).asEagerSingleton();
        bind(ListBlobResourcesAction.class).asEagerSingleton();
    }
}
