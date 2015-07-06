package org.mitallast.queue.raft.resource;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.StateMachine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ResourceRegistry extends AbstractComponent {
    private final Map<Class<? extends Resource>, Class<? extends StateMachine>> resources = new HashMap<>();

    @Inject
    @SuppressWarnings("unchecked")
    public ResourceRegistry(Settings settings) throws IOException {
        super(settings);
        ClassPath classPath = ClassPath.from(getClass().getClassLoader());
        ImmutableSet<ClassPath.ClassInfo> classesInfo = classPath.getTopLevelClassesRecursive("org.mitallast.queue.raft.resource.structures");
        for (ClassPath.ClassInfo classInfo : classesInfo) {
            Class<?> resourceClass = classInfo.load();
            if (Resource.class.isAssignableFrom(resourceClass)) {
                register((Class<? extends Resource>) resourceClass);
            }
        }
    }

    public <T extends Resource> ResourceRegistry register(Class<? extends Resource> resourceType) {
        Stateful stateful = resourceType.getAnnotation(Stateful.class);
        if (stateful != null) {
            register(resourceType, stateful.value());
        } else {
            throw new IllegalArgumentException("unknown resource state: " + resourceType);
        }
        return this;
    }

    public ResourceRegistry register(Class<? extends Resource> resourceType, Class<? extends StateMachine> stateMachineType) {
        resources.put(resourceType, stateMachineType);
        logger.info("Registered resource {} with state machine: {}", resourceType, stateMachineType);
        return this;
    }

    public Class<? extends StateMachine> lookup(Class<? extends Resource> resource) {
        return resources.get(resource);
    }
}
