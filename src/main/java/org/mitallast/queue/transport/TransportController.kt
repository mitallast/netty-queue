package org.mitallast.queue.transport

import javaslang.collection.HashMap
import javaslang.collection.Map
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.common.codec.Message

@Suppress("UNCHECKED_CAST")
class TransportController {

    @Volatile private var handlerMap: Map<Class<*>, (Message) -> Unit> = HashMap.empty()

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

    companion object {
        private val logger = LogManager.getLogger()
    }
}
