package org.mitallast.queue.log;

import com.google.inject.Inject;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.log.entry.TextLogEntry;

public class LogStreamService {
    @Inject
    public LogStreamService(StreamService streamService) {
        int i = 9000000;
        streamService.registerClass(TextLogEntry.Builder.class, TextLogEntry::builder, ++i);
    }
}
