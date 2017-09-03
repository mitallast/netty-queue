package org.mitallast.queue.security

import com.google.inject.AbstractModule
import org.mitallast.queue.common.codec.Codec

class SecurityModule : AbstractModule() {

    override fun configure() {
        bind(SecurityService::class.java).asEagerSingleton()
    }

    companion object {
        init {
            Codec.register(10, ECDHRequest::class.java, ECDHRequest.codec)
            Codec.register(11, ECDHResponse::class.java, ECDHResponse.codec)
            Codec.register(12, ECDHEncrypted::class.java, ECDHEncrypted.codec)
        }
    }
}
