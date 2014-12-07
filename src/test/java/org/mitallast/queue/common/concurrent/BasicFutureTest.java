package org.mitallast.queue.common.concurrent;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BasicFutureTest {

    @Test(timeout = 100)
    public void testPlain() throws Exception {
        BasicFuture<String> future = new BasicFuture<>();
        future.set("foo");
        Assert.assertEquals("foo", future.get());
    }

    @Test(timeout = 100)
    public void testThread() throws Exception {
        final BasicFuture<String> future = new BasicFuture<>();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    future.set("foo");
                } catch (InterruptedException e) {
                    assert false : e;
                }
            }
        });
        thread.start();
        Assert.assertEquals("foo", future.get());
    }

    @Test(timeout = 100)
    public void testThreadSpeed() throws Exception {
        final int max = 100000;
        final List<BasicFuture<String>> futureList = new ArrayList<>(max);
        for (int i = 0; i < max; i++) {
            futureList.add(new BasicFuture<String>());
        }
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (BasicFuture<String> future : futureList) {
                        future.set("foo");
                    }
                } catch (InterruptedException e) {
                    assert false : e;
                }
            }
        });
        thread.start();
        for (BasicFuture<String> future : futureList) {
            Assert.assertEquals("foo", future.get());
        }
    }
}
