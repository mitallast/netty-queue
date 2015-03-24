package org.mitallast.queue.common;

import java.util.concurrent.locks.LockSupport;

public class Locks {
    public static void parkNanos() {
        LockSupport.parkNanos(10);
    }
}
