package org.mitallast.queue.security;

import com.google.inject.AbstractModule;
import org.mitallast.queue.common.codec.Codec;

public class SecurityModule extends AbstractModule {
    static {
        Codec.register(10, ECDHRequest.class, ECDHRequest.codec);
        Codec.register(11, ECDHResponse.class, ECDHResponse.codec);
        Codec.register(12, ECDHEncrypted.class, ECDHEncrypted.codec);
    }

    @Override
    protected void configure() {
        bind(SecurityService.class).asEagerSingleton();
    }
}
