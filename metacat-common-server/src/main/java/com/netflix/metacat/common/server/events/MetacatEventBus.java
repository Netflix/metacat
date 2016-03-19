package com.netflix.metacat.common.server.events;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.SubscriberExceptionHandler;

import java.util.concurrent.Executor;

public class MetacatEventBus extends AsyncEventBus {
    public MetacatEventBus(String identifier, Executor executor) {
        super(identifier, executor);
    }

    public MetacatEventBus(Executor executor,
            SubscriberExceptionHandler subscriberExceptionHandler) {
        super(executor, subscriberExceptionHandler);
    }

    public MetacatEventBus(Executor executor) {
        super(executor);
    }
}
