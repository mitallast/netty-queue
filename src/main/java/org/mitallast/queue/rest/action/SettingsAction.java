package org.mitallast.queue.rest.action;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.rest.RestController;

public class SettingsAction extends AbstractComponent {

    @Inject
    public SettingsAction(Config config, RestController controller) {
        super(config, SettingsAction.class);

        controller.handler(this::settings)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "_settings");
    }

    public ImmutableMap<String, Boolean> settings() {
        return ImmutableMap.of(
            "raft", config.getBoolean("raft.enabled"),
            "blob", config.getBoolean("blob.enabled"),
            "benchmark", config.getBoolean("benchmark.enabled"),
            "crdt", config.getBoolean("crdt.enabled")
        );
    }
}
