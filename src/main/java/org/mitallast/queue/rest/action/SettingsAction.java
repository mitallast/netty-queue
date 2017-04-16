package org.mitallast.queue.rest.action;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.handler.codec.http.HttpMethod;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import org.mitallast.queue.rest.RestController;

public class SettingsAction {

    private final Config config;

    @Inject
    public SettingsAction(Config config, RestController controller) {
        this.config = config;

        controller.handler(this::settings)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "_settings");
    }

    public Map<String, Boolean> settings() {
        return HashMap.of(
            "raft", config.getBoolean("raft.enabled"),
            "crdt", config.getBoolean("crdt.enabled")
        );
    }
}
