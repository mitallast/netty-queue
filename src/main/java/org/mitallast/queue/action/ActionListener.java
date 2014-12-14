package org.mitallast.queue.action;

import org.mitallast.queue.common.concurrent.Listener;

public interface ActionListener<Response extends ActionResponse> extends Listener<Response> {
}
