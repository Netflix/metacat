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
import com.netflix.metacat.common.server.connectors.exception.TableLocationUpdateLockException
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

    private MetacatRequestContext buildContextWithCaller(String caller) {
        def ctx = MetacatRequestContext.builder().build()
        ctx.getAdditionalContext().put(MetacatRequestContext.SSO_DIRECT_CALLER_APP_NAME, caller)
        return ctx
    }

    def "create - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.create(context, resource)

        then:
        1 * delegate.create(context, resource)
    }

    def "create - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc", "irc-server"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
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
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)

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
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.update(context, resource)

        then:
        1 * delegate.update(context, resource)
    }

    def "update - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
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
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.delete(context, name)

        then:
        1 * delegate.delete(context, name)
    }

    def "delete - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("hive"))

        when:
        service.delete(context, name)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.delete(_, _)
    }

    def "get - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.get(context, name)

        then:
        1 * delegate.get(context, name)
    }

    def "get - unauthorized caller throws exception"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("presto"))

        when:
        service.get(context, name)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.get(_, _)
    }

    def "exists - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.exists(context, name)

        then:
        1 * delegate.exists(context, name)
    }

    def "list - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.list(context, name, null, null, null)

        then:
        1 * delegate.list(context, name, null, null, null)
    }

    def "listNames - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.listNames(context, name, null, null, null)

        then:
        1 * delegate.listNames(context, name, null, null, null)
    }

    def "rename - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.rename(context, name, newName)

        then:
        1 * delegate.rename(context, name, newName)
    }

    def "getTableNames with filter - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.getTableNames(context, name, "filter", 10)

        then:
        1 * delegate.getTableNames(context, name, "filter", 10)
    }

    def "getTableNames with URIs - authorized caller succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))

        when:
        service.getTableNames(context, ["s3://bucket/path"], false)

        then:
        1 * delegate.getTableNames(context, ["s3://bucket/path"], false)
    }

    def "exception message contains caller name when unauthorized"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("bad-actor"))

        when:
        service.get(context, name)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("bad-actor")
        ex.message.contains("ads")
    }

    def "exception message indicates missing caller when no SsoDirectCallerAppName"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)

        def ctx = MetacatRequestContext.builder().build()
        MetacatContextManager.setContext(ctx)

        when:
        service.get(context, name)

        then:
        def ex = thrown(CatalogUnauthorizedException)
        ex.message.contains("authenticated caller")
        ex.message.contains("ads")
    }

    def "caller matching is case sensitive - uppercase caller rejected"() {
        given:
        def allowedCallers = ["irc"] as Set  // lowercase
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("IRC"))  // uppercase

        when:
        service.get(context, name)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.get(_, _)
    }

    def "caller matching is case sensitive - lowercase caller accepted"() {
        given:
        def allowedCallers = ["irc"] as Set  // lowercase
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))  // lowercase matches

        when:
        service.get(context, name)

        then:
        1 * delegate.get(context, name)
    }

    def "multiple allowed callers - second caller in list succeeds"() {
        given:
        def allowedCallers = ["irc", "irc-server", "spark"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, false)
        MetacatContextManager.setContext(buildContextWithCaller("spark"))

        when:
        service.get(context, name)

        then:
        1 * delegate.get(context, name)
    }

    // --- Location update lock tests ---

    def "update with metadata_location and lock enabled throws TableLocationUpdateLockException"() {
        given:
        def allowedCallers = [] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, false, true)
        def lockedResource = new TableInfo(name: name, metadata: ["metadata_location": "s3://bucket/new-path"])

        when:
        service.update(context, lockedResource)

        then:
        def ex = thrown(TableLocationUpdateLockException)
        ex.message.contains(catalogName)
        ex.message.contains(name.toString())
        0 * delegate.update(_, _)
    }

    def "update without metadata_location and lock enabled passes through"() {
        given:
        def allowedCallers = [] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, false, true)
        def safeResource = new TableInfo(name: name, metadata: ["some_other_key": "value"])

        when:
        service.update(context, safeResource)

        then:
        1 * delegate.update(context, safeResource)
    }

    def "update with null metadata and lock enabled passes through"() {
        given:
        def allowedCallers = [] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, false, true)
        def noMetadataResource = new TableInfo(name: name)

        when:
        service.update(context, noMetadataResource)

        then:
        1 * delegate.update(context, noMetadataResource)
    }

    def "update with metadata_location and lock disabled passes through"() {
        given:
        def allowedCallers = [] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, false, false)
        def lockedResource = new TableInfo(name: name, metadata: ["metadata_location": "s3://bucket/new-path"])

        when:
        service.update(context, lockedResource)

        then:
        1 * delegate.update(context, lockedResource)
    }

    def "lock enabled with auth disabled - non-update operations pass through without auth check"() {
        given:
        def allowedCallers = [] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, false, true)
        // No caller context set — would fail auth if auth were enabled

        when:
        service.create(context, resource)

        then:
        1 * delegate.create(context, resource)

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

    def "both auth and lock enabled - unauthorized caller blocked by auth before lock check"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, true)
        MetacatContextManager.setContext(buildContextWithCaller("unauthorized-app"))
        def lockedResource = new TableInfo(name: name, metadata: ["metadata_location": "s3://bucket/new-path"])

        when:
        service.update(context, lockedResource)

        then:
        thrown(CatalogUnauthorizedException)
        0 * delegate.update(_, _)
    }

    def "both auth and lock enabled - authorized caller blocked by lock on metadata_location update"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, true)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))
        def lockedResource = new TableInfo(name: name, metadata: ["metadata_location": "s3://bucket/new-path"])

        when:
        service.update(context, lockedResource)

        then:
        thrown(TableLocationUpdateLockException)
        0 * delegate.update(_, _)
    }

    def "both auth and lock enabled - authorized caller update without metadata_location succeeds"() {
        given:
        def allowedCallers = ["irc"] as Set
        service = new AuthorizingConnectorTableService(delegate, allowedCallers, catalogName, true, true)
        MetacatContextManager.setContext(buildContextWithCaller("irc"))
        def safeResource = new TableInfo(name: name, metadata: ["some_key": "value"])

        when:
        service.update(context, safeResource)

        then:
        1 * delegate.update(context, safeResource)
    }
}
