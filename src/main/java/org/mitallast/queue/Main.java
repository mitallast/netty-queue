package org.mitallast.queue;

import org.mitallast.queue.action.queues.create.CreateQueueAction;
import org.mitallast.queue.action.queues.enqueue.EnQueueAction;
import org.mitallast.queue.action.queues.remove.RemoveQueueAction;
import org.mitallast.queue.action.queues.stats.QueuesStatsAction;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.client.QueueClient;
import org.mitallast.queue.client.QueuesClient;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.InternalQueuesService;
import org.mitallast.queue.queues.QueuesService;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.action.RestIndexAction;
import org.mitallast.queue.rest.action.queue.enqueue.RestEnQueueAction;
import org.mitallast.queue.rest.action.queues.create.RestCreateQueueAction;
import org.mitallast.queue.rest.action.queues.remove.RestRemoveQueueAction;
import org.mitallast.queue.rest.action.queues.stats.RestQueuesStatsAction;
import org.mitallast.queue.transport.http.HttpServer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String... args) throws IOException, InterruptedException {
        Settings settings = ImmutableSettings.builder()
                .put("work_dir", "data")
                .build();

        ExecutorService executorService = Executors.newFixedThreadPool(8);

        QueuesService queuesService = new InternalQueuesService(settings);

        QueuesStatsAction queuesStatsAction = new QueuesStatsAction(settings, executorService, queuesService);
        CreateQueueAction createQueueAction = new CreateQueueAction(settings, executorService, queuesService);
        RemoveQueueAction removeQueueAction = new RemoveQueueAction(settings, executorService, queuesService);
        EnQueueAction enQueueAction = new EnQueueAction(settings, executorService, queuesService);

        QueuesClient queuesClient = new QueuesClient(queuesStatsAction, createQueueAction, removeQueueAction);
        QueueClient queueClient = new QueueClient(enQueueAction);
        Client client = new Client(queuesClient, queueClient);


        RestController restController = new RestController(settings);

        new RestEnQueueAction(settings, client, restController);

        new RestIndexAction(settings, client, restController);
        new RestQueuesStatsAction(settings, client, restController);
        new RestCreateQueueAction(settings, client, restController);
        new RestRemoveQueueAction(settings, client, restController);

        new HttpServer(settings, restController).run();
    }
}
