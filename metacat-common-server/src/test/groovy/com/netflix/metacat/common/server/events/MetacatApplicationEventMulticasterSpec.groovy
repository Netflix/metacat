package com.netflix.metacat.common.server.events

import org.springframework.context.ApplicationEvent
import org.springframework.context.event.ApplicationEventMulticaster
import spock.lang.Specification

/**
 * MetacatApplicationEventMulticaster tests.
 *
 * @author amajumdar
 * @since 1.0.0
 */
class MetacatApplicationEventMulticasterSpec extends Specification {
    def asyncEventMulticaster = Mock(ApplicationEventMulticaster)
    def bus = new MetacatApplicationEventMulticaster(asyncEventMulticaster)
    def event = Mock(ApplicationEvent)

    def testPostSync() {
        when:
        bus.postSync(event)
        then:
        0 * asyncEventMulticaster.multicastEvent(event)
    }

    def testPostAsync() {
        when:
        bus.postAsync(event)
        then:
        1 * asyncEventMulticaster.multicastEvent(event)
    }
}
