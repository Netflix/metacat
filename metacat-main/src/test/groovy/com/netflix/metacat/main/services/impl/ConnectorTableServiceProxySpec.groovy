/*
 *  Copyright 2018 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.main.services.impl

import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.ConnectorTableService
import com.netflix.metacat.common.server.connectors.exception.ConnectorException
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.main.manager.ConnectorManager
import com.netflix.metacat.testdata.provider.DataDtoProvider
import spock.lang.Specification

/**
 * Tests for the ConnectorTableServiceProxy.
 *
 * @author amajumdar
 * @since 1.2.0
 */
class ConnectorTableServiceProxySpec extends Specification {
    def connectorManager = Mock(ConnectorManager)
    def connectorTableService = Mock(ConnectorTableService)
    def converterUtil = Mock(ConverterUtil)
    def tableDto = DataDtoProvider.getTable('a', 'b', 'c', "amajumdar", "s3:/a/b")
    def name = tableDto.name
    ConnectorTableServiceProxy service

    def setup() {
        connectorManager.getTableService(_) >> connectorTableService
        converterUtil.toTableDto(_) >> tableDto
        converterUtil.toConnectorContext(_) >> Mock(ConnectorRequestContext)
        service = new ConnectorTableServiceProxy(connectorManager, converterUtil)
    }

    def testCreate() {
        when:
        service.create(name, tableDto)
        then:
        noExceptionThrown()
        1 * connectorTableService.create(_,_)
        when:
        service.create(name, tableDto)
        then:
        thrown(ConnectorException)
        1 * connectorTableService.create(_,_) >> { throw new ConnectorException("Exception") }
    }

    def testDelete() {
        when:
        service.delete(name)
        then:
        noExceptionThrown()
        1 * connectorTableService.delete(_,_)
        when:
        service.delete(name)
        then:
        thrown(ConnectorException)
        1 * connectorTableService.delete(_,_) >> { throw new ConnectorException("Exception") }
    }

    def testGet() {
        when:
        service.get(name, false)
        then:
        noExceptionThrown()
        1 * connectorTableService.get(_,_)
        when:
        service.get(name, false)
        then:
        thrown(ConnectorException)
        1 * connectorTableService.get(_,_) >> { throw new ConnectorException("Exception") }
    }

    def testRename() {
        when:
        service.rename(name, name, false)
        then:
        noExceptionThrown()
        1 * connectorTableService.rename(_,_,_)
        when:
        service.rename(name, name, false)
        then:
        thrown(ConnectorException)
        1 * connectorTableService.rename(_,_,_) >> { throw new ConnectorException("Exception") }
    }

    def testUpdate() {
        when:
        service.update(name, tableDto)
        then:
        noExceptionThrown()
        1 * connectorTableService.update(_,_)
        when:
        service.update(name, tableDto)
        then:
        thrown(ConnectorException)
        1 * connectorTableService.update(_,_) >> { throw new ConnectorException("Exception") }
    }

    def testExists() {
        when:
        service.exists(name)
        then:
        noExceptionThrown()
        1 * connectorTableService.exists(_,_)
        when:
        service.exists(name)
        then:
        thrown(ConnectorException)
        1 * connectorTableService.exists(_,_) >> { throw new ConnectorException("Exception") }
    }
}
