package com.netflix.metacat.common.server.events

import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
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
            final Registry registry
        ) {

            return factory.Spy(MetacatApplicationEventMulticaster, constructorArgs:[registry, new MetacatProperties()]);
        }

        @Bean
        Registry registry() {
            return factory.Mock(Registry);
        }

        @Bean
        EventHandler eventHandler() {
            return factory.Mock(EventHandler);
        }

        @Bean
        AsyncEventHandler asyncEventHandler() {
            return factory.Mock(AsyncEventHandler);
        }
    }
    static class EventHandler {
        @EventListener
        public void post(ApplicationEvent event) {}
    }
    @AsyncListener
    static class AsyncEventHandler {
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
    EventHandler eventHandler
    @Autowired
    AsyncEventHandler asyncEventHandler
    ApplicationEvent event = Mock(ApplicationEvent)
    def test() {
        when:
        eventBus.post(event)
        then:
        1 * applicationEventMulticaster.post(event)
        1 * eventHandler.post(event)
        1 * asyncEventHandler.post(event)
    }
}
