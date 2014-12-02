package org.mitallast.queue.action;


import java.util.concurrent.ExecutionException;

public class FutureActionListener<Response extends ActionResponse> implements ActionListener<Response> {

    private Response response;
    private Throwable error;

    @Override
    public void onResponse(Response response) {
        this.response = response;
    }

    @Override
    public void onFailure(Throwable error) {
        this.error = error;
    }

    public Response get() throws ExecutionException {
        if (error != null) {
            throw new ExecutionException(error);
        }
        return response;
    }
}
