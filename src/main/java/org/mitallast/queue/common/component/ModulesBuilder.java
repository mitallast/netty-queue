package org.mitallast.queue.common.component;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.ArrayList;
import java.util.List;

public class ModulesBuilder {
    List<Module> modules = new ArrayList<>();

    public ModulesBuilder add(Module... modules) {
        for (Module module : modules) {
            add(module);
        }
        return this;
    }

    public ModulesBuilder add(Module module) {
        modules.add(module);
        return this;
    }

    public Injector createInjector() {
        return Guice.createInjector(modules);
    }

    public Injector createChildInjector(Injector injector) {
        return injector.createChildInjector(modules);
    }
}
