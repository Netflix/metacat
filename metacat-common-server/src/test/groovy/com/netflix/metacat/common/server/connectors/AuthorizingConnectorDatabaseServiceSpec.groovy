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
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo
import spock.lang.Specification

class AuthorizingConnectorDatabaseServiceSpec extends Specification {
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
        delegate = Mock(ConnectorDatabaseService)
        authorizer = Mock(ConnectorAuthorizer)
        context = Mock(ConnectorRequestContext)

        catalogName = "ads"
        authorizedCallers = "caller-a,caller-b"
        name = QualifiedName.ofDatabase(catalogName, "db")
        newName = QualifiedName.ofDatabase(catalogName, "db2")
        resource = new DatabaseInfo(name: name)

        service = new AuthorizingConnectorDatabaseService(delegate, authorizer, authorizedCallers, catalogName)
    }

    def "delegates each operation when the authorizer permits the caller"() {
        given:
        authorizer.isAuthorized(catalogName, authorizedCallers, _, _) >> true

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
        service.listViewNames(context, name)
        then:
        1 * delegate.listViewNames(context, name)
    }

    def "throws and does not delegate when the authorizer denies the caller"() {
        given:
        authorizer.isAuthorized(catalogName, authorizedCallers, _, _) >> false

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

    def "passes the operation and resource through to the authorizer"() {
        when:
        service.get(context, name)

        then:
        1 * authorizer.isAuthorized(catalogName, authorizedCallers, "get", name) >> true
        1 * delegate.get(context, name)
    }
}
