package com.netflix.metacat.common.server.events

import com.netflix.spectator.api.NoopRegistry
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.ApplicationEventMulticaster
import org.springframework.context.event.EventListener
import org.springframework.context.event.SimpleApplicationEventMulticaster
import org.springframework.core.task.SyncTaskExecutor
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.support.AnnotationConfigContextLoader
import spock.lang.Specification
import spock.mock.DetachedMockFactory

/**
 * MetacatEventBus integration tests.
 *
 * @author amajumdar
 * @since 1.1.x
 */
@ContextConfiguration(loader=AnnotationConfigContextLoader)
class MetacatEventBusIntegrationSpec extends Specification {
    @Configuration
    static class Config {
        def factory = new DetachedMockFactory()
        @Bean
        MetacatEventBus eventBus(
            final MetacatApplicationEventMulticaster applicationEventMulticaster
        ) {
            return new MetacatEventBus(applicationEventMulticaster, new NoopRegistry());
        }

        @Bean
        MetacatApplicationEventMulticaster applicationEventMulticaster(
            final ApplicationEventMulticaster asyncEventMulticaster
        ) {

            return factory.Spy(MetacatApplicationEventMulticaster, constructorArgs:[asyncEventMulticaster]);
        }

        @Bean
        ApplicationEventMulticaster asyncEventMulticaster() {
            def multicaster = new SimpleApplicationEventMulticaster()
            multicaster.setTaskExecutor(new SyncTaskExecutor())
            return factory.Spy(multicaster)
        }

        @Bean
        EventHandler eventHandler() {
            return factory.Mock(EventHandler);
        }
    }
    static class EventHandler {
        @EventListener
        public void post(ApplicationEvent event) {}
    }
    @Autowired
    ApplicationContext applicationContext
    @Autowired
    MetacatEventBus eventBus
    @Autowired
    MetacatApplicationEventMulticaster applicationEventMulticaster
    @Autowired
    ApplicationEventMulticaster asyncEventMulticaster
    @Autowired
    EventHandler eventHandler
    ApplicationEvent event = Mock(ApplicationEvent)
    def test() {
        when:
        eventBus.postSync(event)
        then:
        1 * applicationEventMulticaster.postSync(event)
        0 * asyncEventMulticaster.multicastEvent(event)
        1 * eventHandler.post(event)
        when:
        eventBus.postAsync(event)
        then:
        1 * applicationEventMulticaster.postAsync(event)
        1 * asyncEventMulticaster.multicastEvent(event)
        1 * eventHandler.post(event)
    }
}
