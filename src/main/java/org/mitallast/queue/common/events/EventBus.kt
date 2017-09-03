package org.mitallast.queue.common.events

import java.util.concurrent.Executor

interface EventBus {

    fun <Event : Any> subscribe(eventClass: Class<Event>, consumer: (Event) -> Unit)

    fun <Event : Any> subscribe(eventClass: Class<Event>, consumer: (Event) -> Unit, executor: Executor)

    fun <Event : Any> unsubscribe(eventClass: Class<Event>, consumer: (Event) -> Unit)

    fun <Event : Any> trigger(event: Event)
}
