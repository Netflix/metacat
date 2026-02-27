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
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo
import com.netflix.metacat.common.server.util.MetacatContextManager
import spock.lang.Specification

class AuthorizingConnectorDatabaseServiceSpec extends Specification {
    def delegate
    def context
    def resource
    def name
    def newName
    def catalogName
    def service

    def setup() {
        delegate = Mock(ConnectorDatabaseService)
        context = Mock(ConnectorRequestContext)

        catalogName = "ads"
        name = QualifiedName.ofDatabase(catalogName, "db")
        newName = QualifiedName.ofDatabase(catalogName, "db2")
        resource = new DatabaseInfo(name: name)

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
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.create(context, resource)

        then:
        1 * delegate.create(context, resource)
    }

    def "create - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
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
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)

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
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.update(context, resource)

        then:
        1 * delegate.update(context, resource)
    }

    def "update - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("spark"))

        when:
        service.update(context, resource)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.update(_, _)
    }

    def "delete - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.delete(context, name)

        then:
        1 * delegate.delete(context, name)
    }

    def "get - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.get(context, name)

        then:
        1 * delegate.get(context, name)
    }

    def "exists - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.exists(context, name)

        then:
        1 * delegate.exists(context, name)
    }

    def "list - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.list(context, name, null, null, null)

        then:
        1 * delegate.list(context, name, null, null, null)
    }

    def "listNames - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.listNames(context, name, null, null, null)

        then:
        1 * delegate.listNames(context, name, null, null, null)
    }

    def "rename - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.rename(context, name, newName)

        then:
        1 * delegate.rename(context, name, newName)
    }

    def "listViewNames - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.listViewNames(context, name)

        then:
        1 * delegate.listViewNames(context, name)
    }

    def "exception message contains caller name when unauthorized"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorDatabaseService(delegate, allowedCallers, catalogName)
        MetacatContextManager.setContext(buildContextWithCaller("bad-actor"))

        when:
        service.get(context, name)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("bad-actor")
        ex.message.contains("ads")
    }
}
