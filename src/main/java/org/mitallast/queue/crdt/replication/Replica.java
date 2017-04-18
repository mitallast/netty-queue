package org.mitallast.queue.crdt.replication;

import com.google.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.crdt.CrdtService;
import org.mitallast.queue.crdt.bucket.Bucket;
import org.mitallast.queue.crdt.log.LogEntry;
import org.mitallast.queue.crdt.protocol.AppendEntries;
import org.mitallast.queue.crdt.protocol.AppendRejected;
import org.mitallast.queue.crdt.protocol.AppendSuccessful;
import org.mitallast.queue.crdt.routing.RoutingBucket;
import org.mitallast.queue.crdt.routing.RoutingReplica;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;

public class Replica {
    private final static Logger logger = LogManager.getLogger();
    private final TransportService transportService;
    private final CrdtService crdtService;

    @Inject
    public Replica(
        TransportService transportService,
        TransportController transportController,
        CrdtService crdtService
    ) {
        this.transportService = transportService;
        this.crdtService = crdtService;

        transportController.registerMessageHandler(AppendEntries.class, this::handle);
    }

    private synchronized void handle(AppendEntries message) {
        Bucket bucket = crdtService.bucket(message.bucket());
        if (bucket == null) {
            logger.warn("unexpected bucket {}, ignore", message.bucket());
            return;
        }
        RoutingBucket routingBucket = crdtService.routingTable().bucket(message.bucket());
        RoutingReplica replica = routingBucket.replicas().getOrElse(message.replica(), null);
        if (replica == null) {
            logger.warn("unexpected replica {}, ignore", message.replica());
            return;
        }

        long localVclock = bucket.vclock().get(message.replica());
        if (localVclock == message.prevVclock()) {
            logger.debug("append entries vclock:{}", localVclock);
            for (LogEntry logEntry : message.entries()) {
                bucket.registry().crdt(logEntry.id()).update(logEntry.event());
                localVclock = Math.max(localVclock, logEntry.vclock());
            }
            bucket.vclock().put(message.replica(), localVclock);
            logger.debug("append entries successful:{}", localVclock);
            transportService.send(
                replica.member(),
                new AppendSuccessful(message.bucket(), bucket.replica(), localVclock)
            );
        } else {
            logger.warn("unmatched vclock local:{} remote:{}", localVclock, message.prevVclock());
            transportService.send(
                replica.member(),
                new AppendRejected(message.bucket(), bucket.replica(), localVclock)
            );
        }
    }
}
