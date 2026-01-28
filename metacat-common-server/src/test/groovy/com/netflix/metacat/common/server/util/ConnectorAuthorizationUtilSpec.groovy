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

    def "checkAuthorization - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        noExceptionThrown()
    }

    def "checkAuthorization - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        def ctx = MetacatRequestContext.builder()
            .userName("unauthorized-app")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("unauthorized-app")
        ex.message.contains(catalogName)
    }

    def "checkAuthorization - null caller throws exception"() {
        given:
        def allowedCallers = ["irc"] as Set
        def ctx = MetacatRequestContext.builder()
            // No userName set - will be null
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("authenticated user")
        ex.message.contains(catalogName)
    }

    def "checkAuthorization - empty allowed callers set denies all"() {
        given:
        def allowedCallers = [] as Set
        def ctx = MetacatRequestContext.builder()
            .userName("irc")
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("irc")
        ex.message.contains(catalogName)
    }

    def "checkAuthorization - case sensitive matching"() {
        given:
        def allowedCallers = ["irc"] as Set  // lowercase
        def ctx = MetacatRequestContext.builder()
            .userName("IRC")  // uppercase
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        thrown(CatalogUnauthorizedException)
    }

    def "checkAuthorization - exact match required"() {
        given:
        def allowedCallers = ["irc-server"] as Set
        def ctx = MetacatRequestContext.builder()
            .userName("irc")  // partial match
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        thrown(CatalogUnauthorizedException)
    }

    def "checkAuthorization - multiple allowed callers - any match succeeds"() {
        given:
        def allowedCallers = ["app1", "app2", "app3"] as Set
        def ctx = MetacatRequestContext.builder()
            .userName(caller)
            .build()
        MetacatContextManager.setContext(ctx)

        when:
        ConnectorAuthorizationUtil.checkAuthorization(catalogName, allowedCallers, operation, resource)

        then:
        noExceptionThrown()

        where:
        caller << ["app1", "app2", "app3"]
    }
}
