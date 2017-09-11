package com.netflix.metacat.common.server.events

import com.netflix.spectator.api.NoopRegistry
import org.springframework.context.ApplicationEvent
import org.springframework.context.event.ApplicationEventMulticaster
import spock.lang.Specification

/**
 * MetacatEventBus tests.
 *
 * @author amajumdar
 * @since 1.0.0
 */
class MetacatEventBusSpec extends Specification {
    def asyncEventMulticaster = Mock(ApplicationEventMulticaster)
    def eventMulticaster = Spy(MetacatApplicationEventMulticaster, constructorArgs:[asyncEventMulticaster])
    def bus = new MetacatEventBus(eventMulticaster, new NoopRegistry())
    def event = Mock(ApplicationEvent)

    def testPostSync() {
        when:
        bus.postSync(event)
        then:
        1 * eventMulticaster.postSync(event) >> {callRealMethod()}
        0 * asyncEventMulticaster.multicastEvent(event)
    }

    def testPostAsync() {
        when:
        bus.postAsync(event)
        then:
        1 * eventMulticaster.postAsync(event) >> {callRealMethod()}
        1 * asyncEventMulticaster.multicastEvent(event)
    }
}
