package com.sc.zhaoqi.common.sync;

import com.google.common.base.Function;
import com.google.common.util.concurrent.*;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;

/**
 * @author sczhaoqi
 */
public class Promises {

    public static final Logger LOGGER = LoggerFactory.getLogger(Promises.class);

    public static interface TransformFunction<Input, Output>
            extends Function<Input, Output> {
        public Output applyThrowable(Input input)
                throws Throwable;

        @Override
        default public Output apply(Input input) {
            try {
                return applyThrowable(input);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    public interface AsyncTransformFunction<Input, Output>
            extends AsyncFunction<Input, Output> {
    }

    public interface Function2<A, B, R> {
        R apply(A a, B b)
                throws Throwable;
    }

    public interface UncheckedCallable<T>
            extends Callable<T> {
        T callThrowable()
                throws Throwable;

        @Override
        default T call() {
            try {
                return this.callThrowable();
            } catch (Throwable throwable) {
                throw new RuntimeException("fail to call", throwable);
            }
        }
    }

    public interface UncheckedRunnable
            extends Runnable {
        void runThrowable()
                throws Throwable;

        @Override
        default void run() {
            try {
                this.runThrowable();
            } catch (Throwable throwable) {
                throw new RuntimeException("fail to apply", throwable);
            }
        }
    }

    public interface UncheckedConsumer<T>
            extends Consumer<T> {
        void acceptThrowable(T t)
                throws Throwable;

        @Override
        default public void accept(T t) {
            try {
                this.acceptThrowable(t);
            } catch (Throwable throwable) {
                throw new RuntimeException("fail to counsume", throwable);
            }
        }
    }

    public static class PromiseCombiner<A, B, R> {

        final ListenablePromise<A> left;
        final ListenablePromise<B> right;

        PromiseCombiner(ListenablePromise<A> left, ListenablePromise<B> right) {
            this.left = left;
            this.right = right;
        }

        public ListenablePromise<R> call(Function2<A, B, R> function) {
            return left.transformAsync((leftResult) -> {
                return right.transform((rightResult) -> {
                    return function.apply(leftResult, rightResult);
                });
            });
        }
    }

    private static ScheduledExecutorService SCHEDULE_EXECUTOR = Executors.newScheduledThreadPool(1);
    private static ExecutorService BLOCKING_CALL_EXECUTOR = Executors.newCachedThreadPool();
    private static ListeningExecutorService EXECUTOR = MoreExecutors.listeningDecorator(
            Executors.newWorkStealingPool(
                    Math.max(8, Runtime.getRuntime().availableProcessors())
            )
    );

    public static <T> ListenablePromise<T> submit(UncheckedCallable<T> callable) {
        return decorate(executor().submit(callable));
    }

    public static ListenablePromise<?> submit(UncheckedRunnable runnable) {
        return decorate(executor().submit(runnable));
    }

    public static ListenablePromise<?> schedule(UncheckedRunnable runnable, long period, boolean auto_resume) {
        CallbackResistedListenablePromise<?> resisted = new CallbackResistedListenablePromise<>(
                decorate(
                        JdkFutureAdapters.listenInPoolThread(
                                SCHEDULE_EXECUTOR.scheduleAtFixedRate(
                                        runnable,
                                        0,
                                        period,
                                        TimeUnit.MILLISECONDS
                                ),
                                BLOCKING_CALL_EXECUTOR
                        )
                )
        );

        // on failure
        resisted.logException((throwable) -> {
            // restart
            if (auto_resume) {
                Promises.delay(
                        () -> {
                            // cut from old promise
                            ListenableFuture<?> future = JdkFutureAdapters.listenInPoolThread(
                                    SCHEDULE_EXECUTOR.scheduleAtFixedRate(
                                            runnable,
                                            0,
                                            period,
                                            TimeUnit.MILLISECONDS
                                    ),
                                    BLOCKING_CALL_EXECUTOR
                            );
                            future.addListener(() -> {
                                try {
                                    Object result = Futures.getUnchecked(future);
                                    resisted.callbacks().parallelStream().forEach((callback) -> ((FutureCallback) callback).onSuccess(result));
                                } catch (Throwable exception) {
                                    resisted.callbacks().parallelStream().forEach((callback) -> ((FutureCallback) callback).onFailure(exception));
                                }
                            }, Promises.executor());
                            return future;
                        },
                        period
                );
            }
            return "fail when scheduling, restart...";
        });

        return resisted;
    }

    public static <T> ListenablePromise<T> delay(UncheckedCallable<T> callable, long delayMiliseconds) {
        return Promises.decorate(
                Futures.scheduleAsync(
                        () -> submit(callable),
                        delayMiliseconds,
                        TimeUnit.MILLISECONDS,
                        SCHEDULE_EXECUTOR
                )
        );
    }

    public static <T> ListenablePromise<T> immediate(T value) {
        return decorate(Futures.immediateFuture(value));
    }

    public static <T> ListenablePromise<T> failure(Throwable throwable) {
        return decorate(Futures.immediateFailedFuture(throwable));
    }

    public static <T> ListenablePromise<T> decorate(ListenableFuture<T> target) {
        return new ListenablePromise<>(target);
    }

    public static <A, B, R> PromiseCombiner<A, B, R> chain(ListenablePromise<A> a, ListenablePromise<B> b, Class<R> typeInference) {
        return new PromiseCombiner<A, B, R>(a, b);
    }

    static ListeningExecutorService executor() {
        return EXECUTOR;
    }

    public static <T> ListenablePromise<T> retry(UncheckedCallable<Optional<T>> invokable, RetryPolicy policy) {
        return retry(invokable, policy, 0);
    }

    public static <T> ListenablePromise<T> retry(UncheckedCallable<Optional<T>> invokable) {
        return retry(invokable,
                // retry with exponential backoff , up to max retries unless no exception specified
                new RetryPolicy() {
                    protected final int MAX_RETRIES = 5;
                    protected final int SLEEP_INTERVAL = 200;
                    protected final RetryPolicy delegate = RetryPolicies.exponentialBackoffRetry(MAX_RETRIES, SLEEP_INTERVAL, TimeUnit.MILLISECONDS);
                    protected final ListenablePromise<RetryAction> spin_retry = Promises.submit(
                            // add one more retrys to make sleep consisitent
                            () -> RetryPolicies.exponentialBackoffRetry(
                                    MAX_RETRIES + 1,
                                    SLEEP_INTERVAL,
                                    TimeUnit.MILLISECONDS
                            ).shouldRetry(null, MAX_RETRIES, 0, true));

                    @Override
                    public RetryAction shouldRetry(Exception e, int retries, int failovers, boolean isIdempotentOrAtMostOnce)
                            throws Exception {
                        RetryAction action = delegate.shouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
                        if (action == RetryAction.FAIL) {
                            if (e == null) {
                                return spin_retry.get();
                            }
                        }

                        return action;
                    }
                });
    }

    public static <T, C extends Collection<T>> BinaryOperator<ListenablePromise<C>> reduceCollectionsOperator() {
        return (left, right) ->
                left.transformAsync((leftCollections) -> right.transformAsync((rightCollections) -> {
                    leftCollections.addAll(rightCollections);
                    return left;
                }));
    }

    protected static <T> ListenablePromise<T> retry(UncheckedCallable<Optional<T>> invokable, RetryPolicy policy, int retried) {
        return retry(invokable, policy, retried, new RuntimeException("stacktrace"));
    }

    protected static <T> ListenablePromise<T> retry(UncheckedCallable<Optional<T>> invokable, RetryPolicy policy, int retried, Throwable stacktrace) {
        SettablePromise<T> future = SettablePromise.create();

        ListenablePromise<Optional<T>> direct_future = submit(invokable).callback((result, exception) -> {
            if (exception == null && result.isPresent()) {
                future.set(result.get());
                return;
            }
            // if should retry?
            RetryPolicy.RetryAction action = null;
            if (exception instanceof Exception) {
                action = policy.shouldRetry((Exception) exception, retried, 0, true);
            } else {
                // ignore exception?
                action = policy.shouldRetry(null, retried, 0, true);
            }
            if (action == RetryPolicy.RetryAction.FAIL) {
                if (exception == null) {
                    exception = new RuntimeException("fail to do invoke:" + invokable + " retried:" + retried + " policy:" + policy, stacktrace);
                }
                future.setException(exception);
                return;
            }
            // canceled
            if (future.isCancelled()) {
                LOGGER.warn("invokable:" + invokable + " cancelled", stacktrace);
                return;
            }
            // do retry
            future.setFuture(
                    delay(
                            () -> retry(invokable, policy, retried + 1, stacktrace),
                            action.delayMillis
                    ).transformAsync((throught) -> throught)
            );
        });
        return future;
    }
}
