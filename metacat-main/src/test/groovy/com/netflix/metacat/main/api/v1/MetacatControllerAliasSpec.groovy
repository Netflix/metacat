/*
 *  Copyright 2026 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.netflix.metacat.main.api.v1

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.exception.MetacatBadRequestException
import com.netflix.metacat.common.server.api.traffic_control.RequestGateway
import com.netflix.metacat.common.exception.MetacatNotFoundException
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.usermetadata.AliasService
import com.netflix.metacat.main.api.RequestWrapper
import com.netflix.metacat.main.services.CatalogService
import com.netflix.metacat.main.services.DatabaseService
import com.netflix.metacat.main.services.MViewService
import com.netflix.metacat.main.services.TableService
import com.netflix.spectator.api.NoopRegistry
import spock.lang.Specification

/**
 * Covers how table endpoints behave when the requested name is an alias.
 */
class MetacatControllerAliasSpec extends Specification {

    def config = Mock(Config)
    def aliasService = Mock(AliasService)
    def requestGateway = Mock(RequestGateway)
    def tableService = Mock(TableService)
    def requestWrapper = new RequestWrapper(new NoopRegistry(), requestGateway)
    def controller = new MetacatController(
        Mock(CatalogService), Mock(DatabaseService), Mock(MViewService),
        tableService, requestWrapper, config)

    def alias = QualifiedName.ofTable("prodhive", "db", "the_alias")
    def source = QualifiedName.ofTable("prodhive", "db", "the_source")

    private static TableDto tableDto(QualifiedName name) {
        def dto = new TableDto()
        dto.setName(name)
        return dto
    }

    def "updateTable hands the alias down untouched, leaving the table service to resolve it"() {
        given: "a request against an alias, with the body named for the alias the caller asked for"
        def body = tableDto(alias)

        when:
        controller.updateTable("prodhive", "db", "the_alias", body, false)

        then: "the controller does not resolve the alias itself"
        0 * aliasService.getTableName(_)
        1 * tableService.updateAndReturn(alias, body, false) >> tableDto(source)
    }

    def "getTable passes the requested name through unresolved, since TableServiceImpl decides whether to resolve it"() {
        when:
        def result = controller.getTable("prodhive", "db", "the_alias", true, true, true, false, false)

        then: "the controller no longer resolves aliases itself"
        0 * aliasService.getTableName(_)
        1 * tableService.get(alias, _) >> Optional.of(tableDto(source))
        result.getName() == source
    }

    def "getTable against an alias 404s when TableServiceImpl leaves it unresolved (includeInfo reads)"() {
        when:
        controller.getTable("prodhive", "db", "the_alias", true, true, true, false, false)

        then: "the connector has no concept of aliases, so an unresolved alias name naturally 404s"
        1 * tableService.get(alias, _) >> Optional.empty()
        thrown(MetacatNotFoundException)
    }

    def "updateTable responds with the source table the service resolved to, never the alias"() {
        given:
        def body = tableDto(alias)

        when:
        def result = controller.updateTable("prodhive", "db", "the_alias", body, false)

        then:
        1 * tableService.updateAndReturn(alias, _, false) >> tableDto(source)
        and: "unlike the old behaviour, the requested alias is not stamped back on"
        result.getName() == source
    }

    def "updateTable dosen't require the body to be named for the table that was requested"() {
        when: "the body names a different table than the path"
        controller.updateTable("prodhive", "db", "the_alias", tableDto(source), false)

        then:
        1 * tableService.updateAndReturn(_, _, _) >> tableDto(source)
    }

    def "updateTable leaves a non-alias name untouched"() {
        given:
        def real = QualifiedName.ofTable("prodhive", "db", "a_real_table")
        def body = tableDto(real)

        when:
        controller.updateTable("prodhive", "db", "a_real_table", body, false)

        then:
        0 * aliasService.getTableName(_)
        1 * tableService.updateAndReturn(real, body, false) >> body
    }

    def "deleteTable hands the alias down untouched, so the table service can refuse it"() {
        when:
        controller.deleteTable("prodhive", "db", "the_alias")

        then: "the controller no longer resolves or rejects aliases itself"
        0 * aliasService.getTableName(_)
        1 * tableService.deleteAndReturn(alias, false) >> tableDto(alias)
    }

    def "createTable hands the alias down untouched, so the table service can refuse it"() {
        when:
        controller.createTable("prodhive", "db", "the_alias", tableDto(alias))

        then: "the controller no longer resolves or rejects aliases itself"
        0 * aliasService.getTableName(_)
        1 * tableService.create(alias, _) >> tableDto(alias)
    }

    def "renameTable hands both names down untouched, so the table service can refuse them"() {
        given:
        def newName = QualifiedName.ofTable("prodhive", "db", "the_new_name")

        when:
        controller.renameTable("prodhive", "db", "the_alias", "the_new_name")

        then: "the controller no longer resolves or rejects aliases itself"
        0 * aliasService.getTableName(_)
        1 * tableService.rename(alias, newName, false)
    }
}
