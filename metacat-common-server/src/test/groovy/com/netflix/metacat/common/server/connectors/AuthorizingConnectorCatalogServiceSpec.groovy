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
import com.netflix.metacat.common.server.connectors.model.CatalogInfo
import com.netflix.metacat.common.server.util.MetacatContextManager
import spock.lang.Specification

class AuthorizingConnectorCatalogServiceSpec extends Specification {
    def delegate
    def context
    def resource
    def name
    def newName
    def catalogName
    def service

    def setup() {
        delegate = Mock(ConnectorCatalogService)
        context = Mock(ConnectorRequestContext)

        catalogName = "ads"
        name = QualifiedName.ofCatalog(catalogName)
        newName = QualifiedName.ofCatalog("ads2")
        resource = new CatalogInfo(name: name)

        // Reset context before each test
        MetacatContextManager.removeContext()
    }

    def cleanup() {
        MetacatContextManager.removeContext()
    }

    def "create - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

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
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

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
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

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
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

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
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

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
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.delete(context, name)

        then:
        1 * delegate.delete(context, name)
    }

    def "get - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.get(context, name)

        then:
        1 * delegate.get(context, name)
    }

    def "exists - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

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
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

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
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

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
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        service.rename(context, name, newName)

        then:
        1 * delegate.rename(context, name, newName)
    }

    def "exception message contains caller name when unauthorized"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorCatalogService(delegate, allowedCallers, catalogName)

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
}
