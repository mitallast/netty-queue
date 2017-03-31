package org.mitallast.queue.crdt.rest;

import com.google.inject.Inject;
import org.mitallast.queue.crdt.CrdtService;
import org.mitallast.queue.rest.RestController;

public class RestCrdtHandler {
    private final CrdtService cmRDTService;

    @Inject
    public RestCrdtHandler(RestController controller, CrdtService cmRDTService) {
        this.cmRDTService = cmRDTService;
    }
}
