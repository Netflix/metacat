/*
 *
 * Copyright 2018 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package com.netflix.metacat.main.services.impl

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.AuditDto
import com.netflix.metacat.common.dto.StorageDto
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.ConnectorTableService
import com.netflix.metacat.common.server.connectors.exception.InvalidMetadataException
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.events.MetacatEventBus
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent
import com.netflix.metacat.common.server.events.MetacatUpdateTablePreEvent
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig
import com.netflix.metacat.common.server.usermetadata.DefaultAuthorizationService
import com.netflix.metacat.common.server.usermetadata.TagService
import com.netflix.metacat.common.server.usermetadata.UserMetadataService
import com.netflix.metacat.common.server.util.MetacatContextManager
import com.netflix.metacat.main.manager.ConnectorManager
import com.netflix.metacat.main.services.DatabaseService
import com.netflix.metacat.main.services.GetTableServiceParameters
import com.netflix.metacat.testdata.provider.DataDtoProvider
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.NoopRegistry
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Tests for the TableServiceImpl.
 *
 * @author amajumdar
 * @since 1.2.0
 */
class TableServiceImplSpec extends Specification {
    def objectMapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setSerializationInclusion(JsonInclude.Include.ALWAYS)

    def connectorManager = Mock(ConnectorManager)
    def connectorTableService = Mock(ConnectorTableService)
    def databaseService = Mock(DatabaseService)
    def tagService = Mock(TagService)
    def usermetadataService = Mock(UserMetadataService)
    def eventBus = Mock(MetacatEventBus)
    def converterUtil = Mock(ConverterUtil)
    def catalogConfig = MetacatCatalogConfig.createFromMapAndRemoveProperties('hive', 'a', ['metacat.interceptor.enabled': 'true', 'metacat.has-data-external':'true'])
    def catalogConfigFalse = MetacatCatalogConfig.createFromMapAndRemoveProperties('hive', 'a', ['metacat.interceptor.enabled': 'false', 'metacat.has-data-external':'false'])
    def registry = new NoopRegistry()
    def config = Mock(Config)
    def tableDto = DataDtoProvider.getTable('a', 'b', 'c', "amajumdar", "s3:/a/b")
    def name = tableDto.name
    def connectorTableServiceProxy
    def authorizationService

    def service
    def setup() {
        config.getMetacatCreateAcl() >> new HashMap<QualifiedName, Set<String>>()
        config.getMetacatDeleteAcl() >> new HashMap<QualifiedName, Set<String>>()
        config.isAuthorizationEnabled() >> true
        connectorManager.getTableService(_) >> connectorTableService
        connectorManager.getCatalogConfig(_) >> catalogConfig
        converterUtil.toTableDto(_) >> tableDto
        converterUtil.toConnectorContext(_) >> new ConnectorRequestContext()
        usermetadataService.getDefinitionMetadata(_) >> Optional.empty()
        usermetadataService.getDataMetadata(_) >> Optional.empty()
        usermetadataService.getDefinitionMetadataWithInterceptor(_,_) >> Optional.empty()
        connectorTableServiceProxy = new ConnectorTableServiceProxy(connectorManager, converterUtil)
        authorizationService = new DefaultAuthorizationService(config)
        service = new TableServiceImpl(connectorManager, connectorTableServiceProxy, databaseService, tagService,
            usermetadataService, new MetacatJsonLocator(),
            eventBus, registry, config, converterUtil, authorizationService)
    }

    def testTableGet() {
        when:
        service.get(name,GetTableServiceParameters.builder().
            disableOnReadMetadataIntercetor(false)
            .includeDataMetadata(true)
            .includeDefinitionMetadata(true)
            .build())
        then:
        1 * connectorManager.getCatalogConfig(_) >> catalogConfig
        1 * usermetadataService.getDefinitionMetadataWithInterceptor(_,_) >> Optional.empty()
        1 * usermetadataService.getDataMetadata(_) >> Optional.empty()
        0 * usermetadataService.getDefinitionMetadata(_) >> Optional.empty()

        when:
        service.get(name,GetTableServiceParameters.builder().
            disableOnReadMetadataIntercetor(true)
            .includeDataMetadata(true)
            .includeDefinitionMetadata(true)
            .build())
        then:
        1 * connectorManager.getCatalogConfig(_) >> catalogConfig
        1 * usermetadataService.getDefinitionMetadata(_) >> Optional.empty()
        1 * usermetadataService.getDataMetadata(_) >> Optional.empty()
        0 * usermetadataService.getDefinitionMetadataWithInterceptor(_,_) >> Optional.empty()
        when:
        service.get(name,GetTableServiceParameters.builder().
            disableOnReadMetadataIntercetor(true)
            .includeDataMetadata(true)
            .includeDefinitionMetadata(true)
            .build())
        then:
        1 * connectorManager.getCatalogConfig(_) >> catalogConfigFalse
        1 * usermetadataService.getDefinitionMetadata(_) >> Optional.empty()
        0 * usermetadataService.getDataMetadata(_)
        0 * usermetadataService.getDefinitionMetadataWithInterceptor(_,_) >> Optional.empty()
    }

    def testIsTableInfoProvided() {
        def name = QualifiedName.ofTable('a','b','c')
        def oldTableDto = new TableDto(name: name, serde: new StorageDto(uri: 's3:/a/b/c'))
        def tableDto = new TableDto(name: name, serde: new StorageDto(uri: 's3:/a/b/c'))
        when:
        def result = service.isTableInfoProvided(tableDto, oldTableDto)
        then:
        !result
        when:
        tableDto.getSerde().setInputFormat('')
        result = service.isTableInfoProvided(tableDto, oldTableDto)
        then:
        result
        when:
        tableDto.getSerde().setInputFormat(null)
        tableDto.getSerde().setUri('s3:/a/b/c1')
        result = service.isTableInfoProvided(tableDto, oldTableDto)
        then:
        result
        when:
        tableDto.setSerde(null)
        result = service.isTableInfoProvided(tableDto, oldTableDto)
        then:
        !result
        when:
        tableDto.setAudit(new AuditDto())
        result = service.isTableInfoProvided(tableDto, oldTableDto)
        then:
        result
    }

    def testDeleteAndReturn() {
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.getNoTableDeleteOnTags() >> []
        1 * config.canDeleteTableDefinitionMetadata() >> true
        1 * usermetadataService.deleteMetadata(_,_)
        0 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.getNoTableDeleteOnTags() >> []
        1 * config.canDeleteTableDefinitionMetadata() >> false
        1 * config.getNamesEnabledForDefinitionMetadataDelete() >> [QualifiedName.fromString('a')]
        1 * usermetadataService.deleteMetadata(_,_)
        0 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.getNoTableDeleteOnTags() >> []
        1 * config.canDeleteTableDefinitionMetadata() >> false
        1 * config.getNamesEnabledForDefinitionMetadataDelete() >> [QualifiedName.fromString('a/b')]
        1 * usermetadataService.deleteMetadata(_,_)
        0 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.getNoTableDeleteOnTags() >> []
        1 * config.canDeleteTableDefinitionMetadata() >> false
        1 * config.getNamesEnabledForDefinitionMetadataDelete() >> [QualifiedName.fromString('a/b/c')]
        1 * usermetadataService.deleteMetadata(_,_)
        0 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.getNoTableDeleteOnTags() >> []
        1 * config.canDeleteTableDefinitionMetadata() >> false
        1 * config.getNamesEnabledForDefinitionMetadataDelete() >> []
        1 * config.canSoftDeleteDataMetadata() >> true
        0 * usermetadataService.deleteMetadata(_,_)
        1 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.getNoTableDeleteOnTags() >> []
        1 * config.canDeleteTableDefinitionMetadata() >> false
        1 * config.getNamesEnabledForDefinitionMetadataDelete() >> [QualifiedName.fromString('a/c')]
        1 * config.canSoftDeleteDataMetadata() >> true
        0 * usermetadataService.deleteMetadata(_,_)
        1 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.getNoTableDeleteOnTags() >> []
        1 * connectorTableService.get(_, _) >> { throw new InvalidMetadataException(name) }
        1 * config.canDeleteTableDefinitionMetadata() >> true
        1 * usermetadataService.deleteMetadata(_,_)
        noExceptionThrown()
    }

    def testUpdateAndReturn() {
        given:
        def updatedTableDto = new TableDto(name: name, serde: new StorageDto(uri: 's3:/a/b/c'))

        when:
        def result = service.updateAndReturn(name, updatedTableDto)

        then:
        2 * usermetadataService.getDefinitionMetadataWithInterceptor(_, _) >> Optional.empty()
        2 * usermetadataService.getDataMetadata(_) >> Optional.empty()
        2 * converterUtil.toTableDto(_) >> this.tableDto
        1 * eventBus.post(_ as MetacatUpdateTablePreEvent)
        1 * eventBus.post({
            MetacatUpdateTablePostEvent e ->
                e.latestCurrentTable && e.currentTable == this.tableDto
        })
        result == this.tableDto
    }

    def "Will not throw on Successful Table Update with Failed Get"() {
        given:
        def updatedTableDto = new TableDto(name: name, serde: new StorageDto(uri: 's3:/a/b/c'))

        when:
        def result = service.updateAndReturn(name, updatedTableDto)

        then:
        1 * converterUtil.toTableDto(_) >> this.tableDto
        1 * converterUtil.toTableDto(_) >> { throw new TableNotFoundException(name) }
        1 * eventBus.post(_ as MetacatUpdateTablePreEvent)
        1 * eventBus.post({
            MetacatUpdateTablePostEvent e ->
                !e.latestCurrentTable && e.currentTable == updatedTableDto
        })
        result == updatedTableDto
        noExceptionThrown()

        when:
        result = service.updateAndReturn(name, updatedTableDto)

        then:
        1 * converterUtil.toTableDto(_) >> this.tableDto
        1 * converterUtil.toTableDto(_) >> { throw new FileNotFoundException("test") }
        1 * eventBus.post(_ as MetacatUpdateTablePreEvent)
        1 * eventBus.post({
            MetacatUpdateTablePostEvent e ->
                !e.latestCurrentTable && e.currentTable == updatedTableDto
        })
        1 * connectorTableService.update(_,_) >> {args -> ((ConnectorRequestContext) args[0]).setIgnoreErrorsAfterUpdate(true)}
        result == updatedTableDto
        noExceptionThrown()
    }

    @Unroll
    def "test ownership diagnostic logging"() {
        given:
        def definitionMetadataJson = toObjectNode(definitionMetadata)
        tableDto = new TableDto(
            name: name,
            definitionMetadata: definitionMetadataJson
        )
        registry = new DefaultRegistry()
        TableServiceImpl tableService = new TableServiceImpl(
            connectorManager, connectorTableServiceProxy, databaseService, tagService,
            usermetadataService, new MetacatJsonLocator(), eventBus, registry, config, converterUtil, authorizationService)

        when:
        tableService.logOwnershipDiagnosticDetails(name, tableDto)

        then:
        registry.counter(
            "unauth.user.create.table",
        "catalog", "a",
        "database", "b",
        "owner", (expectedOwner as String)).count() == 1

        where:
        definitionMetadata                     | expectedOwner
        null                                   | "null"
        "{}"                                   | "null"
        "{\"owner\":null}"                     | "null"
        "{\"owner\":{}}"                       | "null"
        "{\"owner\":{\"userId\":null}}"        | "null"
        "{\"owner\":{\"userId\":\"\"}}"        | "null"
        "{\"owner\":{\"userId\":\" \"}}"       | "null"
        "{\"owner\":{\"userId\":\"metacat\"}}" | "metacat"
        "{\"owner\":{\"userId\":\"root\"}}"    | "root"
    }

    @Unroll
    def "test default attributes"() {
        given:
        def tableService = new TableServiceImpl(
            connectorManager, connectorTableServiceProxy, databaseService, tagService,
            usermetadataService, new MetacatJsonLocator(),
            eventBus, new DefaultRegistry(), config, converterUtil, authorizationService)

        def initialDefinitionMetadataJson = toObjectNode(initialDefinitionMetadata)
        tableDto = new TableDto(
            name: name,
            definitionMetadata: initialDefinitionMetadataJson,
            serde: initialSerde
        )

        MetacatContextManager.getContext().setUserName(sessionUser)

        when:
        tableService.setDefaultAttributes(tableDto)

        then:
        tableDto.getDefinitionMetadata() == toObjectNode(expectedDefMetadata)
        tableDto.getSerde() == expectedSerde

        where:
        initialDefinitionMetadata              | sessionUser | initialSerde                      || expectedDefMetadata                     | expectedSerde
        null                                   | null        | null                              || "{}"                                    | new StorageDto()
        null                                   | "ssarma"    | null                              || "{\"owner\":{\"userId\":\"ssarma\"}}"   | new StorageDto()
        "{\"owner\":{\"userId\":\"ssarma\"}}"  | "asdf"      | new StorageDto(owner: "swaranga") || "{\"owner\":{\"userId\":\"ssarma\"}}"   | new StorageDto(owner: "swaranga")
        "{\"owner\":{\"userId\":\"metacat\"}}" | "ssarma"    | new StorageDto(owner: "swaranga") || "{\"owner\":{\"userId\":\"ssarma\"}}"   | new StorageDto(owner: "swaranga")
        "{\"owner\":{\"userId\":\"root\"}}"    | "ssarma"    | new StorageDto(owner: "swaranga") || "{\"owner\":{\"userId\":\"ssarma\"}}"   | new StorageDto(owner: "swaranga")
        "{\"owner\":{\"userId\":\"root\"}}"    | "metacat"   | new StorageDto(owner: "swaranga") || "{\"owner\":{\"userId\":\"swaranga\"}}" | new StorageDto(owner: "swaranga")
        "{\"owner\":{\"userId\":\"root\"}}"    | "metacat"   | new StorageDto(owner: "metacat")  || "{\"owner\":{\"userId\":\"root\"}}"     | new StorageDto(owner: "metacat")
    }

    ObjectNode toObjectNode(jsonString) {
        return jsonString == null ? null : (objectMapper.readTree(jsonString as String) as ObjectNode)
    }
}
