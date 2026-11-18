package com.netflix.metacat.main.services.init

import com.netflix.metacat.main.services.MetacatThriftService
import spock.lang.Specification

class MetacatThriftInitServiceSpec extends Specification {
    def "can start and stop services in correct order"() {
        given:
        def coreInitService = Mock(MetacatCoreInitService)
        def thriftService = Mock(MetacatThriftService)

        def initializationService = new MetacatThriftInitService(
            thriftService, coreInitService
        )

        when:
        initializationService.start()

        then:
        1 * coreInitService.start()
        1 * thriftService.start()

        when:
        initializationService.stop()

        then:
        1 * thriftService.stop()

        then:
        1 * coreInitService.stop()
    }

    def "stops core services even when stopping thrift fails"() {
        given:
        def coreInitService = Mock(MetacatCoreInitService)
        def thriftService = Mock(MetacatThriftService)
        def initializationService = new MetacatThriftInitService(thriftService, coreInitService)
        initializationService.thriftStarted().set(true)

        when:
        initializationService.stop()

        then:
        1 * thriftService.stop() >> { throw new IllegalStateException("stop failed") }

        then:
        1 * coreInitService.stop()
        !initializationService.thriftStarted().get()
    }

    def "does not start service on exception"() {
        given:
        def coreInitService = Mock(MetacatCoreInitService)
        def thriftService = Mock(MetacatThriftService) {
            1 * start() >> { throw new IllegalArgumentException("uh oh") }
        }

        def initializationService = new MetacatThriftInitService(
            thriftService, coreInitService
        )

        when:
        initializationService.start()

        then:
        thrown(IllegalArgumentException)
    }
}
