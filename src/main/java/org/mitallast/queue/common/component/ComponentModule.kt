package org.mitallast.queue.common.component

import com.google.inject.AbstractModule
import com.typesafe.config.Config
import org.mitallast.queue.common.logging.LoggingService

class ComponentModule(private val config: Config, private val logging: LoggingService) : AbstractModule() {

    override fun configure() {
        val lifecycleService = LifecycleService(logging)
        bind(Config::class.java).toInstance(config)
        bind(LifecycleService::class.java).toInstance(lifecycleService)
        bindListener(LifecycleMatcher(), lifecycleService)
    }
}

