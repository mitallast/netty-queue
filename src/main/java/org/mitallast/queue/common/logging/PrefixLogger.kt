package org.mitallast.queue.common.logging

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.Marker
import org.apache.logging.log4j.message.Message
import org.apache.logging.log4j.spi.ExtendedLogger
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper

internal class PrefixLogger(logger: ExtendedLogger, private val marker: Marker) : ExtendedLoggerWrapper(logger, logger.name, null) {

    override fun logMessage(fqcn: String,
                            level: Level,
                            marker: Marker?,
                            message: Message?,
                            t: Throwable?) {
        assert(marker == null)
        logger.logMessage(fqcn, level, this.marker, message, t)
    }
}
