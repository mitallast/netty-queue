package org.mitallast.index;

import org.mitallast.transport.Request;
import org.mitallast.transport.Response;

import java.io.IOException;

public class IndexController {

    public IndexMessage indexAction(Request request, Response response) throws IOException {
        return request.readValue(IndexMessage.class);
    }
}
