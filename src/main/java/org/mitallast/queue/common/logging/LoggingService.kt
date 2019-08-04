package org.mitallast.queue.common.logging

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.Marker
import org.apache.logging.log4j.spi.ExtendedLogger
import org.apache.logging.log4j.util.StackLocatorUtil

class LoggingService(private val marker: Marker) {
    fun logger(): Logger {
        val logger = LogManager.getLogger(StackLocatorUtil.getCallerClass(2))
        return PrefixLogger(logger as ExtendedLogger, marker)
    }

    fun logger(clazz: Class<*>): Logger {
        val logger = LogManager.getLogger(clazz)
        return PrefixLogger(logger as ExtendedLogger, marker)
    }
}
