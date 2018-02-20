package org.mitallast.queue.common.events

import javaslang.collection.HashMultimap
import javaslang.collection.Multimap
import java.util.concurrent.Executor
import java.util.concurrent.locks.ReentrantLock

@Suppress("UNCHECKED_CAST")
class DefaultEventBus : EventBus {
    private val lock = ReentrantLock()
    @Volatile private var consumers: Multimap<Class<*>, Listener<(Any) -> Unit>> = HashMultimap.withSet<Any>().empty()

    override fun <Event : Any> subscribe(eventClass: Class<Event>, consumer: (Event) -> Unit) {
        lock.lock()
        try {
            consumers = consumers.put(eventClass, Listener(consumer) as Listener<(Any) -> Unit>)
        } finally {
            lock.unlock()
        }
    }

    override fun <Event : Any> subscribe(eventClass: Class<Event>, consumer: (Event) -> Unit, executor: Executor) {
        lock.lock()
        try {
            consumers = consumers.put(eventClass, Listener(consumer, executor) as Listener<(Any) -> Unit>)
        } finally {
            lock.unlock()
        }
    }

    override fun <Event : Any> unsubscribe(eventClass: Class<Event>, consumer: (Event) -> Unit) {
        lock.lock()
        try {
            consumers = consumers.remove(eventClass, Listener(consumer) as Listener<(Any) -> Unit>)
        } finally {
            lock.unlock()
        }
    }

    override fun <Event : Any> trigger(event: Event) {
        consumers.get(event.javaClass).forEach { listeners ->
            listeners.forEach { listener ->
                (listener as Listener<Event>).accept(event)
            }
        }
    }

    private class Listener<in Event : Any> constructor(
        private val consumer: (Event) -> Unit,
        private val executor: Executor? = null) {

        fun accept(event: Event) {
            if (executor == null) {
                consumer.invoke(event)
            } else {
                executor.execute { consumer.invoke(event) }
            }
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as Listener<*>
            if (consumer != other.consumer) return false
            return true
        }

        override fun hashCode(): Int {
            return consumer.hashCode()
        }
    }
}
