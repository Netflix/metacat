package com.netflix.metacat.common.server.events

import com.netflix.metacat.common.MetacatRequestContext
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
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
    def registry = Mock(Registry)
    def eventMulticaster = Spy(MetacatApplicationEventMulticaster, constructorArgs:[registry, new MetacatProperties(null)])
    def bus = new MetacatEventBus(eventMulticaster, new NoopRegistry())
    def event = Mock(ApplicationEvent)

    def testPost() {
        when:
        bus.post(event)
        then:
        1 * eventMulticaster.post(event) >> {callRealMethod()}
    }

    def "does not post events when generation is disabled for the request"() {
        given:
        def context = new MetacatRequestContext()
        context.eventsEnabled = false
        def metacatEvent = new MetacatEvent(QualifiedName.ofTable('a', 'b', 'c'), context, this)

        when:
        bus.post(metacatEvent)

        then:
        0 * eventMulticaster.post(_)
    }
}
