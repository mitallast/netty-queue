package org.mitallast.queue.action;

import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestResponse;

public abstract class GenericAction<Request extends RestRequest, Response extends RestResponse> {
    private final String name;

    /**
     * @param name The name of the rest, must be unique across actions.
     */
    protected GenericAction(String name) {
        this.name = name;
    }

    /**
     * The name of the rest. Must be unique across actions.
     */
    public String name() {
        return this.name;
    }

    /**
     * Creates a new response instance.
     */
    public abstract Response newResponse();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GenericAction that = (GenericAction) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
