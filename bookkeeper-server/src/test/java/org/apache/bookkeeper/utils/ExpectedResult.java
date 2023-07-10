package org.apache.bookkeeper.utils;

// Utility class used to determine the result of a method call
public class ExpectedResult<T> {
    private final T t;
    private final Class<? extends java.lang.Throwable> exception;

    public ExpectedResult(T t, Class<? extends java.lang.Throwable> exception) {
        this.t = t;
        this.exception = exception;
    }

    public T getResult() {
        return t;
    }

    public Class<? extends java.lang.Throwable> getException() {
        return exception;
    }
}