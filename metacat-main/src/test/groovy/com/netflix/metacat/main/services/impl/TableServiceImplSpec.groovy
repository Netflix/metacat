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
import com.netflix.metacat.common.server.model.ChildInfo
import com.netflix.metacat.common.server.model.ParentInfo
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig
import com.netflix.metacat.common.server.usermetadata.DefaultAuthorizationService
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataConstants
import com.netflix.metacat.common.server.usermetadata.TagService
import com.netflix.metacat.common.server.usermetadata.UserMetadataService
import com.netflix.metacat.common.server.util.MetacatContextManager
import com.netflix.metacat.main.manager.ConnectorManager
import com.netflix.metacat.main.services.DatabaseService
import com.netflix.metacat.main.services.GetTableServiceParameters
import com.netflix.metacat.main.services.OwnerValidationService
import com.netflix.metacat.testdata.provider.DataDtoProvider
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.NoopRegistry
import spock.lang.Specification
import spock.lang.Unroll
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService;

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
    def catalogConfig = MetacatCatalogConfig.createFromMapAndRemoveProperties('migration', 'a', ['metacat.interceptor.enabled': 'true', 'metacat.has-data-external':'true', 'metacat.type': 'hive'])
    def catalogConfigFalse = MetacatCatalogConfig.createFromMapAndRemoveProperties('migration', 'a', ['metacat.interceptor.enabled': 'false', 'metacat.has-data-external':'false', 'metacat.type': 'hive'])
    def registry = new NoopRegistry()
    def config = Mock(Config)
    def tableDto = DataDtoProvider.getTable('a', 'b', 'c', "amajumdar", "s3:/a/b")
    def name = tableDto.name
    def connectorTableServiceProxy
    def authorizationService
    def ownerValidationService
    def parentChildRelSvc

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
        connectorTableServiceProxy = Spy(new ConnectorTableServiceProxy(connectorManager, converterUtil))
        authorizationService = new DefaultAuthorizationService(config)
        ownerValidationService = Mock(OwnerValidationService)
        parentChildRelSvc = Mock(ParentChildRelMetadataService)

        service = new TableServiceImpl(connectorManager, connectorTableServiceProxy, databaseService, tagService,
            usermetadataService, new MetacatJsonLocator(),
            eventBus, registry, config, converterUtil, authorizationService, ownerValidationService, parentChildRelSvc)
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
        0 * ownerValidationService.enforceOwnerValidation("updateTable", name, updatedTableDto)

        when:
        updatedTableDto.setDefinitionMetadata(toObjectNode("{\"owner\":{\"userId\":\"ssarma\"}}"))
        service.updateAndReturn(name, updatedTableDto)

        then:
        1 * ownerValidationService.enforceOwnerValidation("updateTable", name, updatedTableDto)

        when:
        tableDto.setDefinitionMetadata(toObjectNode("{\"owner\":{\"userId\":\"ssarma\"}}"))
        service.updateAndReturn(name, updatedTableDto)

        then:
        0 * ownerValidationService.enforceOwnerValidation(_, _, _)
    }

    def "Test Create - Clone Table Fail to create table"() {
        given:
        def childTableName = QualifiedName.ofTable("clone", "clone", "c")
        def parentTableName = QualifiedName.ofTable("clone", "clone", "p")
        def mapper = new ObjectMapper()

        def innerNode = mapper.createObjectNode()
        innerNode.put(ParentChildRelMetadataConstants.PARENT_NAME, "clone/clone/p")
        innerNode.put(ParentChildRelMetadataConstants.PARENT_UUID, "p_uuid")
        innerNode.put("child_table_uuid", "child_uuid")

        def outerNode = mapper.createObjectNode()
        outerNode.set(ParentChildRelMetadataConstants.PARENT_CHILD_RELINFO, innerNode)
        def createTableDto = new TableDto(
            name: childTableName,
            definitionMetadata: outerNode,
            serde: new StorageDto(uri: 's3:/clone/clone/c')
        )
        when:
        service.create(childTableName, createTableDto)
        then:
        1 * ownerValidationService.extractPotentialOwners(_) >> ["cloneClient"]
        1 * ownerValidationService.isUserValid(_) >> true
        1 * ownerValidationService.extractPotentialOwnerGroups(_) >> ["cloneClientGroup"]
        1 * ownerValidationService.isGroupValid(_) >> true

        1 * parentChildRelSvc.createParentChildRelation(parentTableName, "p_uuid", childTableName, "child_uuid", "CLONE")
        1 * connectorTableServiceProxy.create(_, _) >> {throw new RuntimeException("Fail to create")}
        1 * parentChildRelSvc.deleteParentChildRelation(parentTableName, "p_uuid", childTableName, "child_uuid", "CLONE")
        thrown(RuntimeException)
    }

    def "Test Rename - Clone Table Fail to update parent child relation"() {
        given:
        def oldName = QualifiedName.ofTable("clone", "clone", "oldChild")
        def newName = QualifiedName.ofTable("clone", "clone", "newChild")
        when:
        service.rename(oldName, newName, false)

        then:
        1 * config.getNoTableRenameOnTags() >> []
        1 * parentChildRelSvc.rename(oldName, newName)
        1 * connectorTableServiceProxy.rename(oldName, newName, _) >> {throw new RuntimeException("Fail to rename")}
        1 * parentChildRelSvc.rename(newName, oldName)
        thrown(RuntimeException)
    }

    def "Test Drop - Clone Table Fail to drop parent child relation"() {
        given:
        def name = QualifiedName.ofTable("clone", "clone", "child")

        when:
        service.delete(name)
        then:
        1 * parentChildRelSvc.getParents(name) >> {[new ParentInfo("parent", "clone", "parent_uuid")] as Set}
        2 * parentChildRelSvc.getChildren(name) >> {[new ChildInfo("child", "clone", "child_uuid")] as Set}
        1 * config.getNoTableDeleteOnTags() >> []
        thrown(RuntimeException)

        when:
        service.delete(name)
        then:
        1 * parentChildRelSvc.getParents(name)
        2 * parentChildRelSvc.getChildren(name)
        1 * config.getNoTableDeleteOnTags() >> []
        1 * connectorTableServiceProxy.delete(_) >> {throw new RuntimeException("Fail to drop")}
        0 * parentChildRelSvc.drop(_, _)
        thrown(RuntimeException)
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
    def "test default attributes"() {
        given:
        def tableService = new TableServiceImpl(
            connectorManager, connectorTableServiceProxy, databaseService, tagService,
            usermetadataService, new MetacatJsonLocator(),
            eventBus, new DefaultRegistry(), config, converterUtil, authorizationService,
            new DefaultOwnerValidationService(new NoopRegistry()), parentChildRelSvc)

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
        tableDto.getTableOwner() == Optional.ofNullable(expetedFinalOwner)

        where:
        initialDefinitionMetadata                               | sessionUser | initialSerde                      || expectedDefMetadata                     | expectedSerde                     | expetedFinalOwner
        null                                                    | null        | null                              || "{}"                                    | new StorageDto()                  | null
        null                                                    | "ssarma"    | null                              || "{\"owner\":{\"userId\":\"ssarma\"}}"   | new StorageDto()                  | "ssarma"
        null                                                    | "root"      | new StorageDto(owner: "swaranga") || "{\"owner\":{\"userId\":\"swaranga\"}}" | new StorageDto(owner: "swaranga") | "swaranga"
        "{\"owner\":{\"userId\":\"ssarma\"}}"                   | "asdf"      | new StorageDto(owner: "swaranga") || "{\"owner\":{\"userId\":\"ssarma\"}}"   | new StorageDto(owner: "swaranga") | "ssarma"
        "{\"owner\":{\"userId\":\"metacat\"}}"                  | "ssarma"    | new StorageDto(owner: "swaranga") || "{\"owner\":{\"userId\":\"ssarma\"}}"   | new StorageDto(owner: "swaranga") | "ssarma"
        "{\"owner\":{\"userId\":\"root\"}}"                     | "ssarma"    | new StorageDto(owner: "swaranga") || "{\"owner\":{\"userId\":\"ssarma\"}}"   | new StorageDto(owner: "swaranga") | "ssarma"
        "{\"owner\":{\"userId\":\"root\"}}"                     | "metacat"   | new StorageDto(owner: "swaranga") || "{\"owner\":{\"userId\":\"swaranga\"}}" | new StorageDto(owner: "swaranga") | "swaranga"
        "{\"owner\":{\"userId\":\"root\"}}"                     | "metacat"   | new StorageDto(owner: "metacat")  || "{\"owner\":{\"userId\":\"root\"}}"     | new StorageDto(owner: "metacat")  | "root"
        "{\"owner\":{\"userId\":\"metacat-thrift-interface\"}}" | "ssarma"    | new StorageDto(owner: "swaranga") || "{\"owner\":{\"userId\":\"ssarma\"}}"   | new StorageDto(owner: "swaranga") | "ssarma"
    }

    ObjectNode toObjectNode(jsonString) {
        return jsonString == null ? null : (objectMapper.readTree(jsonString as String) as ObjectNode)
    }
}
