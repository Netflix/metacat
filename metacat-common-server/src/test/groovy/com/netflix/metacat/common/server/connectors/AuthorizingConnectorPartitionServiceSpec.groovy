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
import com.netflix.metacat.common.server.connectors.model.PartitionInfo
import spock.lang.Specification

class AuthorizingConnectorPartitionServiceSpec extends Specification {
    def delegate
    def authorizer
    def context
    def resource
    def table
    def name
    def newName
    def catalogName
    def authorizedCallers
    def service

    def setup() {
        delegate = Mock(ConnectorPartitionService)
        authorizer = Mock(ConnectorAuthorizer)
        context = Mock(ConnectorRequestContext)

        catalogName = "catalog1"
        authorizedCallers = "caller-a,caller-b"
        table = QualifiedName.ofTable(catalogName, "db", "tbl")
        name = QualifiedName.ofPartition(catalogName, "db", "tbl", "dateint=1")
        newName = QualifiedName.ofPartition(catalogName, "db", "tbl", "dateint=2")
        resource = new PartitionInfo(name: name)

        service = new AuthorizingConnectorPartitionService(delegate, authorizer, authorizedCallers, catalogName)
    }

    def "delegates each operation when the authorizer permits the caller"() {
        when:
        service.getPartitions(context, table, null, null)
        then:
        1 * delegate.getPartitions(context, table, null, null)

        when:
        service.savePartitions(context, table, null)
        then:
        1 * delegate.savePartitions(context, table, null)

        when:
        service.deletePartitions(context, table, ["dateint=1"], null)
        then:
        1 * delegate.deletePartitions(context, table, ["dateint=1"], null)

        when:
        service.getPartitionKeys(context, table, null, null)
        then:
        1 * delegate.getPartitionKeys(context, table, null, null)

        when:
        service.getPartitionUris(context, table, null, null)
        then:
        1 * delegate.getPartitionUris(context, table, null, null)

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
    }

    def "throws and does not delegate when the authorizer denies the caller"() {
        given:
        authorizer.checkAuthorization(catalogName, authorizedCallers, _, _) >> { throw new CatalogUnauthorizedException(catalogName) }

        when:
        service.savePartitions(context, table, null)
        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.savePartitions(_, _, _)

        when:
        service.get(context, name)
        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.get(_, _)
    }

    def "passes the operation and resource through to the authorizer"() {
        when:
        service.getPartitions(context, table, null, null)

        then:
        1 * authorizer.checkAuthorization(catalogName, authorizedCallers, "getPartitions", table)
        1 * delegate.getPartitions(context, table, null, null)
    }

    def "checks catalog-level access for uri-based getPartitionNames"() {
        when:
        service.getPartitionNames(context, ["s3://uri"], false)

        then:
        1 * authorizer.checkAuthorization(catalogName, authorizedCallers, "getPartitionNames", QualifiedName.ofCatalog(catalogName))
        1 * delegate.getPartitionNames(context, ["s3://uri"], false)
    }
}
