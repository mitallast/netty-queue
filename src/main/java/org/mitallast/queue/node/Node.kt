package org.mitallast.queue.node

import com.google.inject.Injector
import com.typesafe.config.Config
import org.mitallast.queue.common.component.LifecycleComponent

interface Node : LifecycleComponent {

    fun config(): Config

    fun injector(): Injector
}
