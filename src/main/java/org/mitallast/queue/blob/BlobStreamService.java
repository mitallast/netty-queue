package org.mitallast.queue.blob;

import com.google.inject.Inject;
import org.mitallast.queue.blob.protocol.*;
import org.mitallast.queue.common.stream.StreamService;

public class BlobStreamService {

    @Inject
    public BlobStreamService(StreamService streamService) {
        streamService.register(BlobRoutingMap.class, BlobRoutingMap::new, 300);
        streamService.register(PutBlobResource.class, PutBlobResource::new, 301);

        streamService.register(PutBlobResourceRequest.class, PutBlobResourceRequest::new, 302);
        streamService.register(PutBlobResourceResponse.class, PutBlobResourceResponse::new, 303);

        streamService.register(GetBlobResourceRequest.class, GetBlobResourceRequest::new, 304);
        streamService.register(GetBlobResourceResponse.class, GetBlobResourceResponse::new, 305);
    }
}
