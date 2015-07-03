package org.mitallast.queue.action;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.validation.ValidationBuilder;

public interface ActionRequest<E extends ActionRequest> extends Entry<E> {

    ValidationBuilder validate();
}
