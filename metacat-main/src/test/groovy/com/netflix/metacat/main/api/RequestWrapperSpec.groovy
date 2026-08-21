/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.metacat.main.api

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.exception.MetacatBadRequestException
import com.netflix.metacat.common.server.api.traffic_control.RequestGateway
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.usermetadata.AliasService
import com.netflix.metacat.common.server.util.MetacatContextManager
import com.netflix.spectator.api.*
import spock.lang.Specification

import java.util.function.Supplier

class RequestWrapperSpec extends Specification {

    def registry = new NoopRegistry()
    def config = Mock(Config)
    def aliasService = Mock(AliasService)
    def requestGateway = Mock(RequestGateway)
    def counter = Mock(Counter)
    def supplier = Mock(Supplier)
    def requestWrapper

    def setup() {
        this.supplier.get() >> null
        requestWrapper = new RequestWrapper(registry, config, aliasService, requestGateway)
    }

    def "gateway is invoked for each request"() {
        when:
        requestWrapper.processRequest(QualifiedName.fromString("a/b/c"), "getTable", supplier)

        then:
        1 * requestGateway.validateRequest("getTable", QualifiedName.fromString("a/b/c"))
    }

    def "throws the same exception from gateway"() {
        given:
        requestGateway.validateRequest("getTable", QualifiedName.fromString("a/b/c")) >> {
            throw new MetacatBadRequestException("asdf")
        }

        when:
        requestWrapper.processRequest(QualifiedName.fromString("a/b/c"), "getTable", supplier)

        then:
        thrown(MetacatBadRequestException)
    }

    def "requestName is set in the context"() {
        when:
        requestWrapper.processRequest(QualifiedName.fromString("a/b/c"), "getTable", supplier)

        then:
        MetacatContextManager.getContext().getRequestName() == "getTable"
    }

    def "qualifyName resolves a table alias to its source table"() {
        given:
        def alias = QualifiedName.ofTable("c", "d", "the_alias")
        def source = QualifiedName.ofTable("c", "d", "the_source")

        when:
        def result = requestWrapper.qualifyName({ alias })

        then:
        1 * config.isTableAliasEnabled() >> true
        1 * aliasService.getTableName(alias) >> source
        result == source
    }

    def "qualifyName does not consult the alias service when aliasing is disabled"() {
        given:
        def alias = QualifiedName.ofTable("c", "d", "the_alias")

        when:
        def result = requestWrapper.qualifyName({ alias })

        then:
        1 * config.isTableAliasEnabled() >> false
        0 * aliasService.getTableName(_)
        result == alias
    }

    def "qualifyName only resolves aliases for table names"() {
        given:
        def dbName = QualifiedName.ofDatabase("c", "d")

        when:
        def result = requestWrapper.qualifyName({ dbName })

        then:
        0 * aliasService.getTableName(_)
        result == dbName
    }

    def "qualifyName converts a failing supplier into a bad request"() {
        when:
        requestWrapper.qualifyName({ throw new IllegalArgumentException("bad name") })

        then:
        thrown(MetacatBadRequestException)
    }

    def "unresolvedQualifiedName leaves an alias alone, so the table service can decide what to do with it"() {
        given:
        def alias = QualifiedName.ofTable("c", "d", "the_alias")

        when:
        def result = requestWrapper.unresolvedQualifiedName({ alias })

        then:
        0 * aliasService.getTableName(_)
        0 * aliasService.isAlias(_)
        result == alias
    }

    def "unresolvedQualifiedName converts a failing supplier into a bad request"() {
        when:
        requestWrapper.unresolvedQualifiedName({ throw new IllegalArgumentException("bad name") })

        then:
        thrown(MetacatBadRequestException)
    }
}
