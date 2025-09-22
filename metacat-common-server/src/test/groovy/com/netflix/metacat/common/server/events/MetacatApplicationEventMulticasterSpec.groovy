package com.netflix.metacat.common.server.events

import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.spectator.api.Registry
import org.springframework.context.ApplicationEvent
import org.springframework.context.ApplicationListener
import org.springframework.context.event.ApplicationEventMulticaster
import spock.lang.Specification

/**
 * MetacatApplicationEventMulticaster tests.
 *
 * @author amajumdar
 * @since 1.0.0
 */
class MetacatApplicationEventMulticasterSpec extends Specification {
    def registry = Mock(Registry)
    def bus = new MetacatApplicationEventMulticaster(registry, new MetacatProperties())
    def event = Mock(ApplicationEvent)

    def testPost() {
        when:
        bus.post(event)
        then:
        0 * registry.collectionSize(_,_)
        when:
        bus.addApplicationListener(new SyncAppListener())
        bus.post(event)
        then:
        0 * registry.collectionSize(_,_)
        when:
        bus.addApplicationListener(new AsyncAppListener())
        bus.post(event)
        then:
        1 * registry.collectionSize(_,_)
    }

    class SyncAppListener implements ApplicationListener<ApplicationEvent>{

        @Override
        void onApplicationEvent(ApplicationEvent event) {

        }
    }
    @AsyncListener
    class AsyncAppListener implements ApplicationListener<ApplicationEvent>{

        @Override
        void onApplicationEvent(ApplicationEvent event) {

        }
    }
}
