package org.mitallast.queue.action;

import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

public abstract class AbstractAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    public AbstractAction(Settings settings) {
        super(settings);
    }

    public abstract void execute(Request request, ActionListener<Response> listener);
}
