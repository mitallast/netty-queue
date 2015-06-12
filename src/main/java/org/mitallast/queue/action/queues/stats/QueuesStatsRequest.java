package org.mitallast.queue.action.queues.stats;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;

public class QueuesStatsRequest extends ActionRequest {

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder();
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
    }
}
