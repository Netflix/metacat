/*
 *  Copyright 2026 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.netflix.metacat.main.services.impl

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorFactory
import com.netflix.metacat.common.server.connectors.ConnectorFactoryDecorator
import com.netflix.metacat.common.server.connectors.SpringConnectorFactory
import com.netflix.metacat.common.server.connectors.exception.CatalogNotFoundException
import com.netflix.metacat.common.server.usermetadata.AliasService
import com.netflix.metacat.main.manager.ConnectorManager
import spock.lang.Specification

class ConnectorAliasServiceSpec extends Specification {

    def connectorManager = Mock(ConnectorManager)
    def connectorAliasService = new ConnectorAliasService(connectorManager)

    def alias = QualifiedName.ofTable("prodhive", "db", "the_alias")
    def source = QualifiedName.ofTable("prodhive", "db", "the_source")

    private def springFactoryProviding(AliasService aliasService) {
        def springFactory = Mock(SpringConnectorFactory)
        springFactory.getBeanIfAvailable(AliasService) >> aliasService
        def decorator = Mock(ConnectorFactoryDecorator)
        decorator.getDelegate() >> springFactory
        return decorator
    }

    def "delegates resolution to the catalog's connector"() {
        given:
        def polarisAliasService = Mock(AliasService)
        connectorManager.getConnectorFactory(alias) >> springFactoryProviding(polarisAliasService)

        when:
        def result = connectorAliasService.getTableName(alias)

        then:
        1 * polarisAliasService.getTableName(alias) >> source
        result == source
    }

    def "delegates isAlias to the catalog's connector"() {
        given:
        def polarisAliasService = Mock(AliasService)
        connectorManager.getConnectorFactory(alias) >> springFactoryProviding(polarisAliasService)

        when:
        def result = connectorAliasService.isAlias(alias)

        then:
        1 * polarisAliasService.isAlias(alias) >> true
        result
    }

    def "passes the name through when the connector provides no alias service"() {
        given:
        connectorManager.getConnectorFactory(alias) >> springFactoryProviding(null)

        expect:
        connectorAliasService.getTableName(alias) == alias
        !connectorAliasService.isAlias(alias)
    }

    def "passes the name through for a non-spring connector"() {
        given:
        connectorManager.getConnectorFactory(alias) >> Mock(ConnectorFactory)

        expect:
        connectorAliasService.getTableName(alias) == alias
        !connectorAliasService.isAlias(alias)
    }

    def "passes the name through for an unknown catalog rather than raising"() {
        given:
        connectorManager.getConnectorFactory(alias) >> { throw new CatalogNotFoundException("prodhive") }

        expect:
        connectorAliasService.getTableName(alias) == alias
        !connectorAliasService.isAlias(alias)
    }
}
