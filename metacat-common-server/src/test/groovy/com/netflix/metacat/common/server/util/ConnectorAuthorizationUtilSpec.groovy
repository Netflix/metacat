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
package com.netflix.metacat.common.server.util

import com.netflix.metacat.common.MetacatRequestContext
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.exception.CatalogUnauthorizedException
import spock.lang.Specification

class ConnectorAuthorizationUtilSpec extends Specification {

    def catalogName = "test-catalog"
    def resource = QualifiedName.ofTable(catalogName, "db", "table")
    def operation = "get"

    def setup() {
        MetacatContextManager.removeContext()
    }

    def cleanup() {
        MetacatContextManager.removeContext()
    }

    def "checkAuthorization - authorized SsoDirectCallerAppName succeeds"() {
        given:
        def allowedCallers = [new AuthorizedCaller("bdpauthzservice", null), new AuthorizedCaller("irc-server", null)] as Set
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, "bdpauthzservice")
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        noExceptionThrown()
    }

    def "checkAuthorization - unauthorized SsoDirectCallerAppName throws exception"() {
        given:
        def allowedCallers = [new AuthorizedCaller("bdpauthzservice", null), new AuthorizedCaller("irc-server", null)] as Set
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, "unauthorized-app")
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("unauthorized-app")
        ex.message.contains(catalogName)
    }

    def "checkAuthorization - missing SsoDirectCallerAppName throws exception"() {
        given:
        def allowedCallers = [new AuthorizedCaller("bdpauthzservice", null)] as Set
        def ctx = MetacatRequestContext.builder().build()
        // No SsoDirectCallerAppName in additionalContext
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("authenticated caller")
        ex.message.contains(catalogName)
    }

    def "checkAuthorization - empty allowed callers set denies all"() {
        given:
        def allowedCallers = [] as Set
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, "bdpauthzservice")
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("bdpauthzservice")
        ex.message.contains(catalogName)
    }

    def "checkAuthorization - case sensitive matching"() {
        given:
        def allowedCallers = [new AuthorizedCaller("bdpauthzservice", null)] as Set  // lowercase
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, "BDPAUTHZSERVICE")  // uppercase
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        thrown(CatalogUnauthorizedException)
    }

    def "checkAuthorization - exact match required"() {
        given:
        def allowedCallers = [new AuthorizedCaller("bdpauthzservice", null)] as Set
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, "bdpauthz")  // partial match
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        thrown(CatalogUnauthorizedException)
    }

    def "checkAuthorization - multiple allowed callers - any match succeeds"() {
        given:
        def allowedCallers = [new AuthorizedCaller("app1", null), new AuthorizedCaller("app2", null), new AuthorizedCaller("app3", null)] as Set
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, caller)
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        noExceptionThrown()

        where:
        caller << ["app1", "app2", "app3"]
    }

    def "checkAuthorization - compound rule allows when both app and userName match"() {
        given:
        def allowedCallers = [new AuthorizedCaller("hadoop", "idm-worker")] as Set
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, "hadoop")
        ctx.setUserName("idm-worker")
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        noExceptionThrown()
    }

    def "checkAuthorization - compound rule denies when userName does not match"() {
        given:
        def allowedCallers = [new AuthorizedCaller("hadoop", "idm-worker")] as Set
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, "hadoop")
        ctx.setUserName("alice")
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        thrown(CatalogUnauthorizedException)
    }

    def "checkAuthorization - compound rule denies when userName is null"() {
        given:
        def allowedCallers = [new AuthorizedCaller("hadoop", "idm-worker")] as Set
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, "hadoop")
        // userName not set - defaults to null
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        thrown(CatalogUnauthorizedException)
    }

    def "checkAuthorization - mixed list - plain rule still matches any userName"() {
        given:
        def allowedCallers = [
            new AuthorizedCaller("irc", null),
            new AuthorizedCaller("hadoop", "idm-worker")
        ] as Set
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, "irc")
        ctx.setUserName("anyone")
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        noExceptionThrown()
    }

    def "checkAuthorization - compound rule does not match a different app"() {
        given:
        def allowedCallers = [new AuthorizedCaller("hadoop", "idm-worker")] as Set
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, "other")
        ctx.setUserName("idm-worker")
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        thrown(CatalogUnauthorizedException)
    }
}
