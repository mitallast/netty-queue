package org.mitallast.queue.raft;

import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.log.compaction.Compaction;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public abstract class StateMachine extends AbstractLifecycleComponent {
    private final Map<Compaction.Type, Map<Class<? extends Command>, Method>> filters = new HashMap<>();
    private final Map<Class<? extends Operation>, Method> operations = new HashMap<>();
    private Map<Compaction.Type, Method> allFilters = new HashMap<>();
    private Method allOperation;

    protected StateMachine(Settings settings) {
        super(settings);
    }

    @Override
    protected void doStart() throws IOException {
        init(getClass());
    }

    @Override
    protected void doStop() throws IOException {

    }

    @Override
    protected void doClose() throws IOException {

    }

    private void init(Class<?> declaredClass) {
        while (declaredClass != null && declaredClass != Object.class) {
            logger.debug("init with {}", declaredClass);
            Method[] declaredMethods = declaredClass.getDeclaredMethods();
            if (declaredMethods != null) {
                for (Method method : declaredMethods) {
                    declareFilters(method);
                    declareOperations(method);
                }
            }

            Class<?>[] interfaceClasses = declaredClass.getInterfaces();
            if (interfaceClasses != null) {
                for (Class<?> interfaceClass : interfaceClasses) {
                    init(interfaceClass);
                }
            }
            declaredClass = declaredClass.getSuperclass();
        }
    }

    private void declareFilters(Method method) {
        Filter filter = method.getAnnotation(Filter.class);
        if (filter != null) {
            if (method.getReturnType() != Boolean.class && method.getReturnType() != boolean.class) {
                throw new RuntimeException("filter method " + method + " must return boolean");
            }

            method.setAccessible(true);
            for (Class<? extends Command> command : filter.value()) {
                if (command == Filter.All.class) {
                    if (!allFilters.containsKey(filter.compaction())) {
                        allFilters.put(filter.compaction(), method);
                    }
                } else {
                    Map<Class<? extends Command>, Method> filters = this.filters.get(filter.compaction());
                    if (filters == null) {
                        filters = new HashMap<>();
                        this.filters.put(filter.compaction(), filters);
                    }
                    if (!filters.containsKey(command)) {
                        filters.put(command, method);
                    }
                }
            }
        }
    }

    private Method findFilter(Class<? extends Command> type, Compaction.Type compaction) {
        Map<Class<? extends Command>, Method> filters = this.filters.get(compaction);
        if (filters == null) {
            return allFilters.get(compaction);
        }

        Method method = filters.computeIfAbsent(type, t -> {
            for (Map.Entry<Class<? extends Command>, Method> entry : filters.entrySet()) {
                if (entry.getKey().isAssignableFrom(type)) {
                    return entry.getValue();
                }
            }
            return allFilters.get(compaction);
        });

        if (method == null) {
            throw new IllegalArgumentException("unknown command type: " + type);
        }
        return method;
    }

    private void declareOperations(Method method) {
        Apply apply = method.getAnnotation(Apply.class);
        if (apply != null) {
            method.setAccessible(true);
            for (Class<? extends Operation> operation : apply.value()) {
                if (operation == Apply.All.class) {
                    allOperation = method;
                } else if (!operations.containsKey(operation)) {
                    if (!Streamable.class.isAssignableFrom(method.getReturnType())) {
                        logger.warn("invalid method return type {}#{}", method.getDeclaringClass().getName(), method.getName());
                        throw new RuntimeException("invalid method return type: " + method);
                    }
                    operations.put(operation, method);
                }
            }
        }
    }

    private Method findOperation(Class<? extends Operation> type) {
        Method method = operations.computeIfAbsent(type, t -> {
            for (Map.Entry<Class<? extends Operation>, Method> entry : operations.entrySet()) {
                if (entry.getKey().isAssignableFrom(type)) {
                    return entry.getValue();
                }
            }
            return allOperation;
        });

        if (method == null) {
            throw new IllegalArgumentException("unknown operation type: " + type);
        }
        return method;
    }

    public boolean filter(Commit<? extends Command> commit, Compaction compaction) {
        logger.debug("filter {}", commit);
        try {
            return (boolean) findFilter(commit.type(), compaction.type()).invoke(this, commit);
        } catch (IllegalAccessException | InvocationTargetException e) {
            logger.error("internal error", e);
            throw new ApplicationException("failed to filter command");
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Streamable> T apply(Commit<? extends Operation<T>> commit) {
        logger.trace("apply {}", commit);
        try {
            return (T) findOperation(commit.type()).invoke(this, commit);
        } catch (IllegalAccessException | InvocationTargetException e) {
            logger.error("error apply {}", commit, e);
            throw new ApplicationException("failed to invoke operation");
        }
    }
}
