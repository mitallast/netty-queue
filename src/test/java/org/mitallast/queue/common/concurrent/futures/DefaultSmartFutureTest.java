package org.mitallast.queue.common.concurrent.futures;

import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

import java.util.ArrayList;
import java.util.List;

public class DefaultSmartFutureTest extends BaseTest {
    @Test(timeout = 100)
    public void testPlain() throws Exception {
        SmartFuture<String> future = Futures.future();
        future.invoke("foo");
        Assert.assertEquals("foo", future.get());
    }

    @Test(timeout = 100)
    public void testThread() throws Exception {
        SmartFuture<String> future = Futures.future();
        executeAsync(() -> future.invoke("foo"));
        Assert.assertEquals("foo", future.get());
    }

    @Test(timeout = 100)
    public void testThreadSpeed() throws Exception {
        final int max = 100000;
        final List<SmartFuture<String>> futureList = new ArrayList<>(max);
        for (int i = 0; i < max; i++) {
            futureList.add(Futures.future());
        }
        executeAsync(() -> {
            for (SmartFuture<String> future : futureList) {
                future.invoke("foo");
            }
        });
        for (SmartFuture<String> future : futureList) {
            Assert.assertEquals("foo", future.get());
        }
    }
}
