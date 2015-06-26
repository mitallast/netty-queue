package org.mitallast.queue.common.concurrent.futures;

import org.junit.Test;
import org.mitallast.queue.common.BaseBenchmark;
import org.mitallast.queue.common.concurrent.Listener;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class FuturesBenchmark extends BaseBenchmark {

    private int max = 1000000;

    @Test(timeout = 1000)
    public void testCreateSmartFuture() throws Exception {
        for (int i = 0; i < max; i++) {
            Futures.future();
        }
    }

    @Test(timeout = 1000)
    public void testInvokeSmartFuture() throws Exception {
        for (int i = 0; i < max; i++) {
            SmartFuture<String> future = Futures.future();
            future.invoke("foo");
        }
    }

    @Test(timeout = 1000)
    public void testInvokeCallbackSmartFuture() throws Exception {
        Listener<String> listener = new Listener<String>() {
            @Override
            public void onResponse(String result) {

            }

            @Override
            public void onFailure(Throwable e) {

            }
        };
        for (int i = 0; i < max; i++) {
            SmartFuture<String> future = Futures.future();
            future.on(listener);
            future.invoke("foo");
        }
    }

    @Test(timeout = 1000)
    public void testGetSmartFuture() throws Exception {
        for (int i = 0; i < max; i++) {
            SmartFuture<String> future = Futures.future();
            future.invoke("foo");
            future.get();
        }
    }

    @Test(timeout = 1000)
    public void testCreateCompletableFuture() throws Exception {
        for (int i = 0; i < max; i++) {
            CompletableFuture<String> future = new CompletableFuture<>();
        }
    }

    @Test(timeout = 1000)
    public void testInvokeCompletableFuture() throws Exception {
        for (int i = 0; i < max; i++) {
            CompletableFuture<String> future = new CompletableFuture<>();
            future.complete("foo");
        }
    }

    @Test(timeout = 1000)
    public void testInvokeCallbackCompletableFuture() throws Exception {
        BiConsumer<String, Throwable> callback = (s, throwable) -> {
        };
        for (int i = 0; i < max; i++) {
            CompletableFuture<String> future = new CompletableFuture<>();
            future.whenComplete(callback);
            future.complete("foo");
        }
    }

    @Test(timeout = 1000)
    public void testGetCompletableFuture() throws Exception {
        for (int i = 0; i < max; i++) {
            CompletableFuture<String> future = new CompletableFuture<>();
            future.complete("foo");
            future.get();
        }
    }
}
