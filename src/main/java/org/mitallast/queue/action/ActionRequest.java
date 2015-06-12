package org.mitallast.queue.action;

import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.common.validation.ValidationBuilder;

public abstract class ActionRequest implements Streamable {

    public abstract ValidationBuilder validate();
}
