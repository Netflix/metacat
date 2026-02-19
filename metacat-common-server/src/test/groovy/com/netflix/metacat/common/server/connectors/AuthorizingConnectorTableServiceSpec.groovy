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
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.util.MetacatContextManager
import spock.lang.Specification

class AuthorizingConnectorTableServiceSpec extends Specification {
    def delegate
    def context
    def resource
    def name
    def newName
    def catalogName
    def service

    class Success extends RuntimeException {}

    def setup() {
        delegate = Mock(ConnectorTableService)
        context = Mock(ConnectorRequestContext)

        catalogName = "ads"
        name = QualifiedName.ofTable(catalogName, "d", "t")
        newName = QualifiedName.ofTable(catalogName, "d", "t2")
        resource = new TableInfo(name: name)

        // Reset context before each test
        MetacatContextManager.removeContext()
    }

    def cleanup() {
        MetacatContextManager.removeContext()
    }

    def "create - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.create(context, resource)

        then:
        1 * delegate.create(context, resource)
    }

    def "create - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("unauthorized-app")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.create(context, resource)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.create(_, _)
    }

    def "create - missing userName throws exception"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        // No userName set
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
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.update(context, resource)

        then:
        1 * delegate.update(context, resource)
    }

    def "update - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("spark")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.update(context, resource)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.update(_, _)
    }

    def "delete - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.delete(context, name)

        then:
        1 * delegate.delete(context, name)
    }

    def "delete - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("hive")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.delete(context, name)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.delete(_, _)
    }

    def "get - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.get(context, name)

        then:
        1 * delegate.get(context, name)
    }

    def "get - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("presto")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.get(context, name)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.get(_, _)
    }

    def "exists - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.exists(context, name)

        then:
        1 * delegate.exists(context, name)
    }

    def "list - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.list(context, name, null, null, null)

        then:
        1 * delegate.list(context, name, null, null, null)
    }

    def "listNames - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.listNames(context, name, null, null, null)

        then:
        1 * delegate.listNames(context, name, null, null, null)
    }

    def "rename - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.rename(context, name, newName)

        then:
        1 * delegate.rename(context, name, newName)
    }

    def "getTableNames with filter - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.getTableNames(context, name, "filter", 10)

        then:
        1 * delegate.getTableNames(context, name, "filter", 10)
    }

    def "getTableNames with URIs - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.getTableNames(context, ["s3://bucket/path"], false)

        then:
        1 * delegate.getTableNames(context, ["s3://bucket/path"], false)
    }

    def "exception message contains caller name when unauthorized"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("bad-actor")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.get(context, name)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("bad-actor")
        ex.message.contains("ads")
    }

    def "exception message indicates missing userName when no caller"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder().build()
        MetacatContextManager.setContext(ctx)

        when:
        service.get(context, name)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("authenticated user")
        ex.message.contains("ads")
    }

    def "caller matching is case sensitive - uppercase caller rejected"() {
        given:
        def allowedCallers = ["irc"] as Set  // lowercase
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("IRC")  // uppercase
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.get(context, name)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.get(_, _)
    }

    def "caller matching is case sensitive - lowercase caller accepted"() {
        given:
        def allowedCallers = ["irc"] as Set  // lowercase
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")  // lowercase matches
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.get(context, name)

        then:
        1 * delegate.get(context, name)
    }

    def "multiple allowed callers - second caller in list succeeds"() {
        given:
        def allowedCallers = ["irc", "irc-server", "spark"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("spark")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.get(context, name)

        then:
        1 * delegate.get(context, name)
    }
}
