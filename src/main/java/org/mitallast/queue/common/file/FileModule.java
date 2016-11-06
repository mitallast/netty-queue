package org.mitallast.queue.common.file;

import com.google.inject.AbstractModule;

public class FileModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(FileService.class).asEagerSingleton();
    }
}
