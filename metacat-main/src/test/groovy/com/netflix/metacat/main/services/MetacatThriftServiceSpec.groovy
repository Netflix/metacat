package com.netflix.metacat.main.services

import com.netflix.metacat.common.server.spi.MetacatCatalogConfig
import com.netflix.metacat.main.manager.ConnectorManager
import com.netflix.metacat.thrift.CatalogThriftService
import com.netflix.metacat.thrift.CatalogThriftServiceFactory
import spock.lang.Specification

class MetacatThriftServiceSpec extends Specification {
    def connectorManager = Mock(ConnectorManager)
    def thriftServiceFactory = Mock(CatalogThriftServiceFactory)
    def thriftService = new MetacatThriftService(thriftServiceFactory, connectorManager)

    def 'start and stop use the same thrift server instance'() {
        given:
        def catalog = catalog('prodhive', 12001)
        def server = Mock(CatalogThriftService)
        connectorManager.catalogConfigs >> [catalog]

        when:
        thriftService.start()

        then:
        1 * thriftServiceFactory.create('prodhive', 12001) >> server
        1 * server.start()
        thriftService.catalogThriftServices == [server]

        when:
        thriftService.stop()

        then:
        1 * server.stop()
        thriftService.catalogThriftServices.empty
        0 * thriftServiceFactory.create(_, _)
    }

    def 'a partial startup failure stops servers that already own ports'() {
        given:
        def testCatalog = catalog('testhive', 12002)
        def prodCatalog = catalog('prodhive', 12001)
        def testServer = Mock(CatalogThriftService)
        def prodServer = Mock(CatalogThriftService)
        connectorManager.catalogConfigs >> [testCatalog, prodCatalog]
        thriftServiceFactory.create('testhive', 12002) >> testServer
        thriftServiceFactory.create('prodhive', 12001) >> prodServer

        when:
        thriftService.start()

        then:
        1 * testServer.start()
        1 * prodServer.start() >> { throw new IllegalStateException('port unavailable') }
        1 * testServer.stop()
        def exception = thrown(IllegalStateException)
        exception.message == 'port unavailable'
        thriftService.catalogThriftServices.empty
    }

    def 'duplicate thrift ports fail before creating or starting servers'() {
        given:
        def testCatalog = catalog('testhive', 12001)
        def prodCatalog = catalog('prodhive', 12001)
        connectorManager.catalogConfigs >> [testCatalog, prodCatalog]

        when:
        thriftService.start()

        then:
        def exception = thrown(IllegalStateException)
        exception.message == 'Catalogs testhive and prodhive are both configured to use thrift port 12001'
        0 * thriftServiceFactory._
    }

    private MetacatCatalogConfig catalog(String name, int port) {
        MetacatCatalogConfig.createFromMapAndRemoveProperties(
            'hive', name, [(MetacatCatalogConfig.Keys.THRIFT_PORT): port.toString()]
        )
    }
}
