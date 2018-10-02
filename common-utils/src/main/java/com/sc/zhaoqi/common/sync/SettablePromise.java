package com.sc.zhaoqi.common.sync;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author sczhaoqi
 */
public class SettablePromise<T>
        extends ListenablePromise<T> {

    private final SettableFuture<T> settable;

    public static <T> SettablePromise<T> create() {
        return new SettablePromise<T>();
    }

    public SettablePromise() {
        super(SettableFuture.create());
        this.settable = (SettableFuture<T>) delegate();
    }

    public boolean set(T value) {
        return settable.set(value);
    }

    boolean setException(Throwable throwable) {
        return settable.setException(throwable);
    }

    boolean setFuture(ListenableFuture<? extends T> future) {
        return settable.setFuture(future);
    }
}
