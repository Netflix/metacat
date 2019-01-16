package com.netflix.metacat.common.server.events

import com.netflix.metacat.common.server.properties.MetacatProperties
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
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
    def registry = new SimpleMeterRegistry()
    def eventMulticaster = Spy(MetacatApplicationEventMulticaster, constructorArgs:[registry, new MetacatProperties()])
    def bus = new MetacatEventBus(eventMulticaster, registry)
    def event = Mock(ApplicationEvent)

    def testPost() {
        when:
        bus.post(event)
        then:
        1 * eventMulticaster.post(event) >> {callRealMethod()}
    }
}
