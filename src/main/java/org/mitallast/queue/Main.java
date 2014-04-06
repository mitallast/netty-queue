package org.mitallast.queue;

import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.action.IndexAction;
import org.mitallast.queue.transport.http.HttpServer;

import java.io.IOException;

public class Main {
    public static void main(String... args) throws IOException, InterruptedException {
        Settings settings = ImmutableSettings.EMPTY;
        RestController restController = new RestController(settings);
        new IndexAction(settings, null, restController);
        new HttpServer(settings, restController).run();
    }
}
