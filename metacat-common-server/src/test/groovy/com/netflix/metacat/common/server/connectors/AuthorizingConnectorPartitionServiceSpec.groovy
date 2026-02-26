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

import com.netflix.metacat.common.MetacatRequestContext
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.exception.CatalogUnauthorizedException
import com.netflix.metacat.common.server.connectors.model.PartitionInfo
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.util.MetacatContextManager
import spock.lang.Specification

class AuthorizingConnectorPartitionServiceSpec extends Specification {
    def delegate
    def context
    def resource
    def tableName
    def partitionName
    def newName
    def catalogName
    def service
    def tableInfo
    def partitionListRequest
    def partitionsSaveRequest

    def setup() {
        delegate = Mock(ConnectorPartitionService)
        context = Mock(ConnectorRequestContext)
        tableInfo = Mock(TableInfo)
        partitionListRequest = Mock(PartitionListRequest)
        partitionsSaveRequest = Mock(PartitionsSaveRequest)

        catalogName = "ads"
        tableName = QualifiedName.ofTable(catalogName, "db", "table")
        partitionName = QualifiedName.ofPartition(catalogName, "db", "table", "dateint=20240101")
        newName = QualifiedName.ofPartition(catalogName, "db", "table", "dateint=20240102")
        resource = new PartitionInfo(name: partitionName)

        // Reset context before each test
        MetacatContextManager.removeContext()
    }

    def cleanup() {
        MetacatContextManager.removeContext()
    }

    private MetacatRequestContext buildContextWithCaller(String caller) {
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, caller)
        return ctx
    }

    def "create - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.create(context, resource)

        then:
        1 * delegate.create(context, resource)
    }

    def "create - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("unauthorized-app"))

        when:
        service.create(context, resource)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.create(_, _)
    }

    def "create - missing SsoDirectCallerAppName throws exception"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)

        // No SsoDirectCallerAppName in additionalContext
        def ctx = MetacatRequestContext.builder().build()
        MetacatContextManager.setContext(ctx)

        when:
        service.create(context, resource)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.create(_, _)
    }

    def "update - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.update(context, resource)

        then:
        1 * delegate.update(context, resource)
    }

    def "delete - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.delete(context, partitionName)

        then:
        1 * delegate.delete(context, partitionName)
    }

    def "get - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.get(context, partitionName)

        then:
        1 * delegate.get(context, partitionName)
    }

    def "exists - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.exists(context, partitionName)

        then:
        1 * delegate.exists(context, partitionName)
    }

    def "list - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.list(context, tableName, null, null, null)

        then:
        1 * delegate.list(context, tableName, null, null, null)
    }

    def "listNames - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.listNames(context, tableName, null, null, null)

        then:
        1 * delegate.listNames(context, tableName, null, null, null)
    }

    def "rename - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.rename(context, partitionName, newName)

        then:
        1 * delegate.rename(context, partitionName, newName)
    }

    def "getPartitions - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.getPartitions(context, tableName, partitionListRequest, tableInfo)

        then:
        1 * delegate.getPartitions(context, tableName, partitionListRequest, tableInfo)
    }

    def "getPartitions - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("spark"))

        when:
        service.getPartitions(context, tableName, partitionListRequest, tableInfo)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.getPartitions(_, _, _, _)
    }

    def "savePartitions - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.savePartitions(context, tableName, partitionsSaveRequest)

        then:
        1 * delegate.savePartitions(context, tableName, partitionsSaveRequest)
    }

    def "savePartitions - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("hive"))

        when:
        service.savePartitions(context, tableName, partitionsSaveRequest)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.savePartitions(_, _, _)
    }

    def "deletePartitions - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        def partitionNames = ["dateint=20240101"]
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.deletePartitions(context, tableName, partitionNames, tableInfo)

        then:
        1 * delegate.deletePartitions(context, tableName, partitionNames, tableInfo)
    }

    def "getPartitionCount - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.getPartitionCount(context, tableName, tableInfo)

        then:
        1 * delegate.getPartitionCount(context, tableName, tableInfo)
    }

    def "getPartitionNames - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        def uris = ["s3://bucket/path"]
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.getPartitionNames(context, uris, false)

        then:
        1 * delegate.getPartitionNames(context, uris, false)
    }

    def "getPartitionKeys - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.getPartitionKeys(context, tableName, partitionListRequest, tableInfo)

        then:
        1 * delegate.getPartitionKeys(context, tableName, partitionListRequest, tableInfo)
    }

    def "getPartitionUris - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.getPartitionUris(context, tableName, partitionListRequest, tableInfo)

        then:
        1 * delegate.getPartitionUris(context, tableName, partitionListRequest, tableInfo)
    }

    def "exception message contains caller name when unauthorized"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("bad-actor"))

        when:
        service.get(context, partitionName)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("bad-actor")
        ex.message.contains("ads")
    }

    def "exception message indicates missing caller when no SsoDirectCallerAppName"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorPartitionService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder().build()
        MetacatContextManager.setContext(ctx)

        when:
        service.get(context, partitionName)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("authenticated caller")
        ex.message.contains("ads")
    }
}
