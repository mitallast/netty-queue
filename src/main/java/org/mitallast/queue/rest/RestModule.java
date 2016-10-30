package org.mitallast.queue.rest;

import com.google.inject.AbstractModule;
import org.mitallast.queue.rest.action.ResourceAction;
import org.mitallast.queue.rest.action.RestIndexAction;
import org.mitallast.queue.rest.action.blob.GetBlobResourceAction;
import org.mitallast.queue.rest.action.blob.ListBlobResourcesAction;
import org.mitallast.queue.rest.action.blob.PutBlobResourceAction;
import org.mitallast.queue.rest.action.raft.RaftLogAction;
import org.mitallast.queue.rest.action.raft.RaftStateAction;
import org.mitallast.queue.rest.transport.HttpServer;

public class RestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(HttpServer.class).asEagerSingleton();
        bind(RestController.class).asEagerSingleton();
        bind(RestIndexAction.class).asEagerSingleton();
        bind(ResourceAction.class).asEagerSingleton();

        bind(RaftStateAction.class).asEagerSingleton();
        bind(RaftLogAction.class).asEagerSingleton();

        bind(PutBlobResourceAction.class).asEagerSingleton();
        bind(GetBlobResourceAction.class).asEagerSingleton();
        bind(ListBlobResourcesAction.class).asEagerSingleton();
    }
}
