package com.sc.zhaoqi.common.sync;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * @param <T>
 * @author sczhaoqi
 */
public class ListenablePromise<T>
        implements ListenableFuture<T> {

    public static final Logger LOG = LoggerFactory.getLogger(ListenablePromise.class);

    protected final ListenableFuture<T> deleagte;

    public ListenablePromise(ListenableFuture<T> deleagte) {
        this.deleagte = deleagte;
    }

    public interface PromiseCallback<T>
            extends FutureCallback<T> {

        void onComplete(T result, Throwable throwable)
                throws Throwable;

        @Override
        default void onSuccess(T result) {
            try {
                onComplete(result, null);
            } catch (Throwable throwable) {
                onFailure(throwable);
            }
        }

        @Override
        default void onFailure(Throwable t) {
            try {
                onComplete(null, t);
            } catch (Throwable throwable) {
                LOG.warn("fail of complete promise:" + this, t);
            }
        }
    }

    public interface PromiseSuccessOnlyCallback<T>
            extends PromiseCallback<T> {

        void callback(T result)
                throws Throwable;

        @Override
        default void onComplete(T result, Throwable t)
                throws Throwable {
            if (t != null) {
                LOG.warn("promise fail:" + this, t);
                return;
            }

            callback(result);
        }
    }

    public <NEW> ListenablePromise<NEW> transform(Promises.TransformFunction<? super T, ? extends NEW> transformer) {
        return Promises.decorate(Futures.transform(this, transformer, Promises.executor()));
    }

    public <NEW> ListenablePromise<NEW> transformAsync(Promises.AsyncTransformFunction<? super T, ? extends NEW> transformer) {
        return Promises.decorate(Futures.transformAsync(this, transformer, Promises.executor()));
    }

    public ListenablePromise<T> callback(PromiseCallback<T> callback) {
        Futures.addCallback(this, callback, Promises.executor());
        return this;
    }

    public ListenablePromise<T> callback(PromiseSuccessOnlyCallback<T> callback) {
        Futures.addCallback(this, callback, Promises.executor());
        return this;
    }

    public ListenablePromise<T> orFail(Supplier<ListenablePromise<T>> whenFail) {
        SettablePromise<T> settable = SettablePromise.create();
        this.callback((result, e) -> {
            if (e == null) {
                settable.set(result);
                return;
            }
            LOG.warn("promise fail:" + this, e);
            settable.setFuture(whenFail.get());
        });

        return settable;
    }

    public ListenablePromise<T> whenFail(Promises.UncheckedRunnable callback) {
        this.callback((ignore, e) -> {
            if (e != null) {
                LOG.warn("promise fail:" + callback, e);
                callback.run();
                return;
            }

            return;
        });

        return this;
    }

    public T unchecked() {
        return Futures.getUnchecked(this);
    }

    public ListenablePromise<T> logException() {
        return logException((ignore) -> "promise fail");
    }

    public ListenablePromise<T> logException(Promises.TransformFunction<Throwable, String> message) {
        return callback((ignore, t) -> {
            if (t != null) {
                LOG.error(message.apply(t), t);
            }
        });
    }

    ListenableFuture<T> delegate() {
        return deleagte;
    }

    @Override
    @Deprecated
    public void addListener(Runnable listener, Executor executor) {
        this.deleagte.addListener(listener, executor);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return this.deleagte.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return this.deleagte.isCancelled();
    }

    @Override
    public boolean isDone() {
        return this.deleagte.isDone();
    }

    @Override
    public T get()
            throws InterruptedException, ExecutionException {
        return this.deleagte.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return this.deleagte.get(timeout, unit);
    }
}
