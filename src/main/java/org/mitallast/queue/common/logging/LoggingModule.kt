package org.mitallast.queue.common.logging

import com.google.inject.AbstractModule

class LoggingModule(private val service: LoggingService) : AbstractModule() {
    override fun configure() {
        bind(LoggingService::class.java).toInstance(service)
    }
}
