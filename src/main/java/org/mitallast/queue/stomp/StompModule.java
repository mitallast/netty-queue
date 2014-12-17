package org.mitallast.queue.stomp;

import com.google.inject.AbstractModule;
import org.mitallast.queue.stomp.action.*;
import org.mitallast.queue.stomp.transport.StompServer;
import org.mitallast.queue.stomp.transport.StompServerHandler;

public class StompModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(StompSubscriptionController.class).asEagerSingleton();
        bind(StompServerHandler.class).asEagerSingleton();
        bind(StompServer.class).asEagerSingleton();

        bind(StompController.class).asEagerSingleton();

        bind(StompAckAction.class).asEagerSingleton();
        bind(StompNackAction.class).asEagerSingleton();
        bind(StompBeginAction.class).asEagerSingleton();

        bind(StompSubscribeAction.class).asEagerSingleton();
        bind(StompUnsubscribeAction.class).asEagerSingleton();

        bind(StompConnectAction.class).asEagerSingleton();
        bind(StompDisconnectAction.class).asEagerSingleton();
        bind(StompSendAction.class).asEagerSingleton();
        bind(StompUnknownAction.class).asEagerSingleton();
    }
}
