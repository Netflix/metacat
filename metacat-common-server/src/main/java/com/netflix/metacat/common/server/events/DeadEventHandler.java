package com.netflix.metacat.common.server.events;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;

@Singleton
public class DeadEventHandler {
    private static final Logger log = LoggerFactory.getLogger(DeadEventHandler.class);

    @Subscribe
    @AllowConcurrentEvents
    public void logDeadEvent(DeadEvent event) {
        Object sourceEvent = event.getEvent();
        Object source = event.getSource();
        log.debug("Unhandled event: {} from source: {}", sourceEvent, source);
    }
}
