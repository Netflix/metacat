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
        def allowedCallers = ["bdpauthzservice", "irc-server"] as Set
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
        def allowedCallers = ["bdpauthzservice", "irc-server"] as Set
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
        def allowedCallers = ["bdpauthzservice"] as Set
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
        def allowedCallers = ["bdpauthzservice"] as Set  // lowercase
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
        def allowedCallers = ["bdpauthzservice"] as Set
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, "bdpauthz")  // partial match
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        thrown(CatalogUnauthorizedException)
    }

    def "checkAuthorization - SKIP_CONNECTOR_AUTHORIZATION flag bypasses auth"() {
        given:
        def allowedCallers = ["bdpauthzservice"] as Set
        def ctx = MetacatRequestContext.builder().build()
        // No SSO caller set, but bypass flag is set
        ctx.getAdditionalContext().put(MetacatRequestContext.SKIP_CONNECTOR_AUTHORIZATION, "true")
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        noExceptionThrown()
    }

    def "checkAuthorization - multiple allowed callers - any match succeeds"() {
        given:
        def allowedCallers = ["app1", "app2", "app3"] as Set
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
}
