package org.mitallast.index;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IndexMessage {
    private String message;

    public IndexMessage(@JsonProperty("message") String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
