package org.apache.bookkeeper.utils;

import lombok.Getter;

// Utility class used to determine the result of a method call
@Getter
public class ExpectedResult<T> {
    private final T t;
    private final Class<? extends java.lang.Throwable> exception;

    public ExpectedResult(T t, Class<? extends java.lang.Throwable> exception) {
        this.t = t;
        this.exception = exception;
    }
}