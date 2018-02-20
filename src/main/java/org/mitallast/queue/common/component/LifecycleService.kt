package org.mitallast.queue.common.component

import com.google.inject.spi.ProvisionListener
import java.util.*

class LifecycleService : AbstractLifecycleComponent(), ProvisionListener {

    private val lifecycleQueue = ArrayList<LifecycleComponent>()

    @Synchronized override fun <T> onProvision(provision: ProvisionListener.ProvisionInvocation<T>) {
        val instance = provision.provision()
        if (instance === this) {
            return
        }
        logger.debug("provision {}", instance)
        val lifecycleComponent = instance as LifecycleComponent
        lifecycleQueue.add(lifecycleComponent)
    }

    @Synchronized override fun doStart() {
        for (component in lifecycleQueue) {
            logger.debug("starting {}", component)
            component.start()
        }
    }

    @Synchronized override fun doStop() {
        val size = lifecycleQueue.size
        for (i in size - 1 downTo 0) {
            val component = lifecycleQueue[i]
            logger.debug("stopping {}", component)
            component.stop()
        }
    }

    @Synchronized override fun doClose() {
        val size = lifecycleQueue.size
        for (i in size - 1 downTo 0) {
            val component = lifecycleQueue[i]
            logger.debug("closing {}", component)
            component.close()
        }
    }
}
