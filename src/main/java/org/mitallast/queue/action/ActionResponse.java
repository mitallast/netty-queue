package org.mitallast.queue.action;

import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;

public interface ActionResponse<B extends EntryBuilder<B, E>, E extends ActionResponse<B, E>> extends Entry<B, E> {
}
