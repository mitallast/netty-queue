package org.mitallast.queue.common.component;

import com.google.inject.spi.ProvisionListener;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;

public class LifecycleService extends AbstractLifecycleComponent implements ProvisionListener {

    private final List<LifecycleComponent> lifecycleQueue;

    protected LifecycleService(Settings settings) {
        super(settings);
        lifecycleQueue = new ArrayList<>();
    }

    @Override
    public synchronized <T> void onProvision(ProvisionInvocation<T> provision) {
        final T instance = provision.provision();
        if (instance == this) {
            return;
        }
        logger.debug("provision {}", instance);
        LifecycleComponent lifecycleComponent = (LifecycleComponent) instance;
        lifecycleQueue.add(lifecycleComponent);
    }

    @Override
    protected void doStart() throws QueueException {
        final int size = lifecycleQueue.size();
        for (int i = 0; i < size; i++) {
            LifecycleComponent component = lifecycleQueue.get(i);
            logger.debug("starting {}", component);
            component.start();
        }
    }

    @Override
    protected void doStop() throws QueueException {
        final int size = lifecycleQueue.size();
        for (int i = size - 1; i >= 0; i--) {
            LifecycleComponent component = lifecycleQueue.get(i);
            logger.debug("stopping {}", component);
            component.stop();
        }
    }

    @Override
    protected void doClose() throws QueueException {
        final int size = lifecycleQueue.size();
        for (int i = size - 1; i >= 0; i--) {
            LifecycleComponent component = lifecycleQueue.get(i);
            logger.debug("closing {}", component);
            component.close();
        }
    }
}
