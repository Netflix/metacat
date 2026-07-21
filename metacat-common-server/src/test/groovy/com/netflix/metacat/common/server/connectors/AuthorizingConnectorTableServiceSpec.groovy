/*
 *
 *  Copyright 2024 Netflix, Inc.
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
 *
 */
package com.netflix.metacat.common.server.connectors

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.exception.CatalogUnauthorizedException
import com.netflix.metacat.common.server.connectors.model.TableInfo
import spock.lang.Specification

class AuthorizingConnectorTableServiceSpec extends Specification {
    def delegate
    def authorizer
    def context
    def resource
    def name
    def newName
    def catalogName
    def authorizedCallers
    def service

    def setup() {
        delegate = Mock(ConnectorTableService)
        authorizer = Mock(ConnectorAuthorizer)
        context = Mock(ConnectorRequestContext)

        catalogName = "catalog1"
        authorizedCallers = "caller-a,caller-b"
        name = QualifiedName.ofTable(catalogName, "db", "tbl")
        newName = QualifiedName.ofTable(catalogName, "db", "tbl2")
        resource = new TableInfo(name: name)

        service = new AuthorizingConnectorTableService(delegate, authorizer, authorizedCallers, catalogName)
    }

    def "delegates each operation when the authorizer permits the caller"() {
        when:
        service.create(context, resource)
        then:
        1 * delegate.create(context, resource)

        when:
        service.update(context, resource)
        then:
        1 * delegate.update(context, resource)

        when:
        service.delete(context, name)
        then:
        1 * delegate.delete(context, name)

        when:
        service.get(context, name)
        then:
        1 * delegate.get(context, name)

        when:
        service.exists(context, name)
        then:
        1 * delegate.exists(context, name)

        when:
        service.list(context, name, null, null, null)
        then:
        1 * delegate.list(context, name, null, null, null)

        when:
        service.listNames(context, name, null, null, null)
        then:
        1 * delegate.listNames(context, name, null, null, null)

        when:
        service.rename(context, name, newName)
        then:
        1 * delegate.rename(context, name, newName)

        when:
        service.getTableNames(context, name, "filter", 10)
        then:
        1 * delegate.getTableNames(context, name, "filter", 10)

        when:
        service.getTableNames(context, ["s3://uri"], false)
        then:
        1 * delegate.getTableNames(context, ["s3://uri"], false)
    }

    def "throws and does not delegate when the authorizer denies the caller"() {
        given:
        authorizer.checkAuthorization(catalogName, authorizedCallers, _, _) >> { throw new CatalogUnauthorizedException(catalogName) }

        when:
        service.create(context, resource)
        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.create(_, _)

        when:
        service.get(context, name)
        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.get(_, _)
    }

    def "checks catalog-level access for uri-based getTableNames"() {
        when:
        service.getTableNames(context, ["s3://uri"], false)

        then:
        1 * authorizer.checkAuthorization(catalogName, authorizedCallers, "getTableNames", QualifiedName.ofCatalog(catalogName))
        1 * delegate.getTableNames(context, ["s3://uri"], false)
    }
}
