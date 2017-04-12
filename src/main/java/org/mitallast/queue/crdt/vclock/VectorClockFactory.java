package org.mitallast.queue.crdt.vclock;

public interface VectorClockFactory {
    VectorClock create(int index);
}
