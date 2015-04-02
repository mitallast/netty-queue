package org.mitallast.queue.common.concurrent.futures;

import org.junit.Test;
import org.mitallast.queue.common.BaseBenchmark;

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
            SmartFuture<Object> future = Futures.future();
            future.invoke("foo");
        }
    }

    @Test(timeout = 1000)
    public void testGetSmartFuture() throws Exception {
        for (int i = 0; i < max; i++) {
            SmartFuture<Object> future = Futures.future();
            future.invoke("foo");
            future.get();
        }
    }
}
