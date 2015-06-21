package org.mitallast.queue.action;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.validation.ValidationBuilder;

public interface ActionRequest<B extends EntryBuilder<B, E>, E extends ActionRequest<B, E>> extends Entry<B, E> {

    ValidationBuilder validate();
}
