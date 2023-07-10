package org.apache.bookkeeper.utils;

public class Pair<T, S> {
    private final T t;
    private final S s;

    public Pair(T t, S s) {
        this.t = t;
        this.s = s;

    }

    public T getFirst() {
        return t;
    }

    public S getSecond() {
        return s;
    }
}