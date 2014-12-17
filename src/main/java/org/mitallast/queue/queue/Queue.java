package org.mitallast.queue.queue;

public class Queue {
    private final String name;

    public Queue(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Queue queue = (Queue) o;

        if (name != null ? !name.equals(queue.name) : queue.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Queue{" +
            "name='" + name + '\'' +
            '}';
    }
}