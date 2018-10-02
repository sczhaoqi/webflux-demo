package com.sc.zhaoqi.webfluxdemo.handler;

import com.sc.zhaoqi.common.sync.Promises;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Component
public class UserHandler {
    public Mono<ServerResponse> helloBody(ServerRequest request){

        try {
            return Promises.submit(() -> LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10))).transform(empty -> ServerResponse.ok().contentType(MediaType.TEXT_PLAIN)
                    .body(BodyInserters.fromObject("Hello, Body!"))).get().retry(10);
        } catch (Exception ignored) {
            // ignored
        }
        return ServerResponse.badRequest().build();
    }
}
