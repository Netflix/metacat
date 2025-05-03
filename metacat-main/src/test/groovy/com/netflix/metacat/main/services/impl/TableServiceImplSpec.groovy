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
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.properties.ParentChildRelationshipProperties
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

    ObjectNode createParentChildRelMetadata(String rootTableName, String rootTableUuid, String childTableUuid) {
        ObjectMapper mapper = new ObjectMapper()
        ObjectNode node = mapper.createObjectNode()

        if (rootTableName != null) {
            node.put(ParentChildRelMetadataConstants.PARENT_NAME, rootTableName)
        }
        if (rootTableUuid != null) {
            node.put(ParentChildRelMetadataConstants.PARENT_UUID, rootTableUuid)
        }
        if (childTableUuid != null) {
            node.put(ParentChildRelMetadataConstants.CHILD_UUID, childTableUuid)
        }

        ObjectNode outerNode = mapper.createObjectNode()
        outerNode.set(ParentChildRelMetadataConstants.PARENT_CHILD_RELINFO, node)

        return outerNode
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
        def result = service.updateAndReturn(name, updatedTableDto, false)

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
        service.updateAndReturn(name, updatedTableDto, false)

        then:
        1 * ownerValidationService.enforceOwnerValidation("updateTable", name, updatedTableDto)

        when:
        tableDto.setDefinitionMetadata(toObjectNode("{\"owner\":{\"userId\":\"ssarma\"}}"))
        service.updateAndReturn(name, updatedTableDto, false)

        then:
        0 * ownerValidationService.enforceOwnerValidation(_, _, _)
    }

    def 'Test invalid Clone parameters' () {
        when:
        def dto = new TableDto()
        def tableName = QualifiedName.ofTable("prodhive", "clone", "child");
        dto.setName(tableName)
        dto.setDefinitionMetadata(createParentChildRelMetadata(parentName, rootTableUuid, childTableUuid))
        service.create(tableName, dto)

        then:
        1 * config.isParentChildCreateEnabled() >> true
        0 * config.getParentChildRelationshipProperties()
        1 * ownerValidationService.extractPotentialOwners(_) >> ["cloneClient"]
        1 * ownerValidationService.isUserValid(_) >> true
        1 * ownerValidationService.extractPotentialOwnerGroups(_) >> ["cloneClientGroup"]
        1 * ownerValidationService.isGroupValid(_) >> true
        _ * connectorTableServiceProxy.exists(_) >> true

        def e = thrown(IllegalArgumentException)
        assert e.message.contains(message)
        where:
        parentName                           | rootTableUuid | childTableUuid | message
        null                                 | "parent_uuid" | "child_uuid"   | "parent name is not specified"
        "prodhive/clone/parent"              | null          | "child_uuid"   | "parent_table_uuid is not specified for parent table=prodhive/clone/parent"
        "prodhive/clone/parent"              | "parent_uuid" | null           | "child_table_uuid is not specified for child table=prodhive/clone/child"
        "prodhive/parent"                    | "parent_uuid" | "child_uuid"   | "does not refer to a table"
    }



    def "Mock Parent Child Relationship Create"() {
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
        def parentChildProps = new ParentChildRelationshipProperties(null)
        // mock case where create Table Fail as parent table does not exist
        when:
        service.create(childTableName, createTableDto)
        then:
        1 * config.isParentChildCreateEnabled() >> true
        0 * config.getParentChildRelationshipProperties()
        1 * ownerValidationService.extractPotentialOwners(_) >> ["cloneClient"]
        1 * ownerValidationService.isUserValid(_) >> true
        1 * ownerValidationService.extractPotentialOwnerGroups(_) >> ["cloneClientGroup"]
        1 * ownerValidationService.isGroupValid(_) >> true
        1 * connectorTableServiceProxy.exists(_) >> false

        0 * parentChildRelSvc.createParentChildRelation(parentTableName, "p_uuid", childTableName, "child_uuid", "CLONE")
        0 * connectorTableServiceProxy.create(_, _) >> {throw new RuntimeException("Fail to create")}
        0 * parentChildRelSvc.deleteParentChildRelation(parentTableName, "p_uuid", childTableName, "child_uuid", "CLONE")
        def e = thrown(RuntimeException)
        assert e.message.contains("does not exist")

        // mock case where create Table Fail and revert function is triggerred
        when:
        service.create(childTableName, createTableDto)
        then:
        1 * config.isParentChildCreateEnabled() >> true
        1 * config.getParentChildRelationshipProperties() >> parentChildProps
        1 * ownerValidationService.extractPotentialOwners(_) >> ["cloneClient"]
        1 * ownerValidationService.isUserValid(_) >> true
        1 * ownerValidationService.extractPotentialOwnerGroups(_) >> ["cloneClientGroup"]
        1 * ownerValidationService.isGroupValid(_) >> true
        1 * connectorTableServiceProxy.exists(_) >> true

        1 * parentChildRelSvc.createParentChildRelation(parentTableName, "p_uuid", childTableName, "child_uuid", "CLONE", parentChildProps)
        1 * connectorTableServiceProxy.create(_, _) >> {throw new RuntimeException("Fail to create")}
        1 * parentChildRelSvc.deleteParentChildRelation(parentTableName, "p_uuid", childTableName, "child_uuid", "CLONE")
        thrown(RuntimeException)

        // mock case where parent child relationship creation is disabled
        when:
        service.create(childTableName, createTableDto)

        then:
        1 * config.isParentChildCreateEnabled() >> false
        0 * config.getParentChildRelationshipProperties()
        1 * ownerValidationService.extractPotentialOwners(_) >> ["cloneClient"]
        1 * ownerValidationService.isUserValid(_) >> true
        1 * ownerValidationService.extractPotentialOwnerGroups(_) >> ["cloneClientGroup"]
        1 * ownerValidationService.isGroupValid(_) >> true
        0 * connectorTableServiceProxy.exists(_)

        0 * parentChildRelSvc.createParentChildRelation(parentTableName, "p_uuid", childTableName, "child_uuid", "CLONE", parentChildProps)
        0 * connectorTableServiceProxy.create(_, _)
        e = thrown(RuntimeException)
        assert e.message.contains("is currently disabled")

        // mock successful case
        when:
        service.create(childTableName, createTableDto)
        then:
        1 * config.isParentChildCreateEnabled() >> true
        1 * config.getParentChildRelationshipProperties() >> parentChildProps
        1 * ownerValidationService.extractPotentialOwners(_) >> ["cloneClient"]
        1 * ownerValidationService.isUserValid(_) >> true
        1 * ownerValidationService.extractPotentialOwnerGroups(_) >> ["cloneClientGroup"]
        1 * ownerValidationService.isGroupValid(_) >> true
        1 * connectorTableServiceProxy.exists(_) >> true

        1 * parentChildRelSvc.createParentChildRelation(parentTableName, "p_uuid", childTableName, "child_uuid", "CLONE", parentChildProps)
        1 * connectorTableServiceProxy.create(_, _)
        0 * parentChildRelSvc.deleteParentChildRelation(parentTableName, "p_uuid", childTableName, "child_uuid", "CLONE")
        noExceptionThrown()
    }

    def "Mock Parent Child Relationship Rename"() {
        given:
        def oldName = QualifiedName.ofTable("clone", "clone", "oldChild")
        def newName = QualifiedName.ofTable("clone", "clone", "newChild")
        // mock when rename fail and revert happen
        when:
        service.rename(oldName, newName, false)

        then:
        1 * config.isParentChildRenameEnabled() >> true
        1 * config.getNoTableRenameOnTags() >> []
        1 * parentChildRelSvc.rename(oldName, newName)
        1 * connectorTableServiceProxy.rename(oldName, newName, _) >> {throw new RuntimeException("Fail to rename")}
        1 * parentChildRelSvc.rename(newName, oldName)
        thrown(RuntimeException)

        // mock when rename parent child relation is disabled and the table is a child table
        when:
        service.rename(oldName, newName, false)
        then:
        1 * config.getNoTableRenameOnTags() >> []
        1 * config.isParentChildRenameEnabled() >> false
        1 * parentChildRelSvc.isChildTable(oldName) >> true
        0 * parentChildRelSvc.isParentTable(oldName)
        0 * parentChildRelSvc.rename(oldName, newName)
        0 * connectorTableServiceProxy.rename(oldName, newName, _)
        def e = thrown(RuntimeException)
        assert e.message.contains("is currently disabled")

        // mock when rename parent child relation is disabled and the table is a parent table
        when:
        service.rename(oldName, newName, false)
        then:
        1 * config.getNoTableRenameOnTags() >> []
        1 * config.isParentChildRenameEnabled() >> false
        1 * parentChildRelSvc.isChildTable(oldName) >> false
        1 * parentChildRelSvc.isParentTable(oldName) >> true
        0 * parentChildRelSvc.rename(oldName, newName)
        0 * connectorTableServiceProxy.rename(oldName, newName, _)
        e = thrown(RuntimeException)
        assert e.message.contains("is currently disabled")

        // mock when rename parent child relation is disabled but the table is not a parent nor child table
        when:
        service.rename(oldName, newName, false)
        then:
        1 * config.getNoTableRenameOnTags() >> []
        1 * config.isParentChildRenameEnabled() >> false
        1 * parentChildRelSvc.isChildTable(oldName) >> false
        1 * parentChildRelSvc.isParentTable(oldName) >> false
        1 * parentChildRelSvc.rename(oldName, newName)
        1 * connectorTableServiceProxy.rename(oldName, newName, _)
        noExceptionThrown()

        // mock normal success case
        when:
        service.rename(oldName, newName, false)
        then:
        1 * config.getNoTableRenameOnTags() >> []
        1 * config.isParentChildRenameEnabled() >> true
        0 * parentChildRelSvc.isChildTable(oldName)
        0 * parentChildRelSvc.isParentTable(oldName)
        1 * parentChildRelSvc.rename(oldName, newName)
        1 * connectorTableServiceProxy.rename(oldName, newName, _)
        noExceptionThrown()
    }

    def "Mock Parent Child Relationship Drop"() {
        given:
        def name = QualifiedName.ofTable("clone", "clone", "child")

        // drop a parent table that has child
        when:
        service.delete(name)
        then:
        1 * config.isParentChildGetEnabled() >> true
        1 * config.isParentChildDropEnabled() >> true
        1 * parentChildRelSvc.getParents(name) >> {[] as Set}
        1 * parentChildRelSvc.isParentTable(name) >> true
        1 * parentChildRelSvc.getChildren(name) >> {[new ChildInfo("child", "clone", "child_uuid")] as Set}
        1 * config.getNoTableDeleteOnTags() >> []
        def e = thrown(RuntimeException)
        assert e.message.contains("because it still has")

        // mock failure to drop the table, should not trigger deletion in parentChildRelSvc
        when:
        service.delete(name)
        then:
        1 * config.isParentChildGetEnabled() >> true
        1 * config.isParentChildDropEnabled() >> true
        1 * parentChildRelSvc.getParents(name)
        1 * parentChildRelSvc.isParentTable(name) >> true
        1 * parentChildRelSvc.getChildren(name)
        1 * config.getNoTableDeleteOnTags() >> []
        1 * connectorTableServiceProxy.delete(_) >> {throw new RuntimeException("Fail to drop")}
        0 * parentChildRelSvc.drop(_)
        thrown(RuntimeException)

        // mock drop is not enabled and it is a child table
        when:
        service.delete(name)
        then:
        1 * config.isParentChildGetEnabled() >> true
        1 * config.isParentChildDropEnabled() >> false
        1 * parentChildRelSvc.isChildTable(name) >> true
        1 * parentChildRelSvc.isParentTable(name) >> false
        1 * parentChildRelSvc.getParents(name)
        0 * parentChildRelSvc.getChildren(name)
        1 * config.getNoTableDeleteOnTags() >> []
        0 * connectorTableServiceProxy.delete(_)
        0 * parentChildRelSvc.drop(_)
        e = thrown(RuntimeException)
        assert e.message.contains("is currently disabled")

        // mock drop is not enabled and it is a parent table
        when:
        service.delete(name)
        then:
        1 * config.isParentChildGetEnabled() >> true
        1 * config.isParentChildDropEnabled() >> false
        1 * parentChildRelSvc.isChildTable(name) >> false
        2 * parentChildRelSvc.isParentTable(name) >> true
        1 * parentChildRelSvc.getParents(name)
        0 * parentChildRelSvc.getChildren(name)
        1 * config.getNoTableDeleteOnTags() >> []
        0 * connectorTableServiceProxy.delete(_)
        0 * parentChildRelSvc.drop(_)
        e = thrown(RuntimeException)
        assert e.message.contains("is currently disabled")

        // mock drop is not enabled and it is not a parent nor child table
        // and it doesn't have any children
        when:
        service.delete(name)
        then:
        1 * config.isParentChildGetEnabled() >> true
        1 * config.isParentChildDropEnabled() >> false
        1 * parentChildRelSvc.isChildTable(name) >> false
        2 * parentChildRelSvc.isParentTable(name) >> false
        1 * parentChildRelSvc.getParents(name) >> {[] as Set}
        1 * parentChildRelSvc.getChildren(name) >> {[] as Set}
        1 * config.getNoTableDeleteOnTags() >> []
        1 * connectorTableServiceProxy.delete(_)
        1 * parentChildRelSvc.drop(_)
        1 * config.canDeleteTableDefinitionMetadata() >> true
        noExceptionThrown()

        // mock normal successful case
        when:
        service.delete(name)
        then:
        1 * config.isParentChildGetEnabled() >> true
        1 * config.isParentChildDropEnabled() >> true
        0 * parentChildRelSvc.isChildTable(name)
        1 * parentChildRelSvc.isParentTable(name)
        1 * parentChildRelSvc.getParents(name) >> {[] as Set}
        1 * parentChildRelSvc.getChildren(name) >> {[] as Set}
        1 * config.getNoTableDeleteOnTags() >> []
        1 * connectorTableServiceProxy.delete(_)
        1 * parentChildRelSvc.drop(_)
        1 * config.canDeleteTableDefinitionMetadata() >> true
        noExceptionThrown()
    }

    def "Mock Parent Child Relationship Get"() {
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
        1 * config.isParentChildGetEnabled() >> false
        0 * parentChildRelSvc.getParents(name)
        0 * parentChildRelSvc.isParentTable(name)

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
        1 * config.isParentChildGetEnabled() >> true
        1 * parentChildRelSvc.getParents(name)
        1 * parentChildRelSvc.isParentTable(name)
    }

    def "Will not throw on Successful Table Update with Failed Get"() {
        given:
        def updatedTableDto = new TableDto(name: name, serde: new StorageDto(uri: 's3:/a/b/c'))

        when:
        def result = service.updateAndReturn(name, updatedTableDto, false)

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
        result = service.updateAndReturn(name, updatedTableDto, false)

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
