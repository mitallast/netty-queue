package org.mitallast.queue.transport

import com.google.inject.Inject
import io.vavr.collection.HashMap
import io.vavr.collection.Map
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.logging.LoggingService

@Suppress("UNCHECKED_CAST")
class TransportController @Inject constructor(logging: LoggingService) {
    private val logger = logging.logger()
    @Volatile
    private var handlerMap: Map<Class<*>, (Message) -> Unit> = HashMap.empty()

    @Synchronized
    fun <T : Message> registerMessageHandler(
        requestClass: Class<T>,
        handler: (T) -> Unit
    ) {
        handlerMap = handlerMap.put(requestClass, handler as ((Message) -> Unit))
    }

    @Synchronized
    fun <T : Message> registerMessagesHandler(
        requestClass: Class<T>,
        handler: (Message) -> Unit
    ) {
        handlerMap = handlerMap.put(requestClass, handler)
    }

    fun <T : Message> dispatch(message: T) {
        val handler = handlerMap.getOrElse(message.javaClass, null)
        if (handler != null) {
            handler.invoke(message)
        } else {
            logger.error("handler not found for {}", message.javaClass)
        }
    }
}
