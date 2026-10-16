package com.netflix.metacat.main.services

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.DefinitionMetadataDto
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.metacat.common.server.usermetadata.AliasService
import com.netflix.metacat.common.server.usermetadata.TagService
import com.netflix.metacat.common.server.usermetadata.UserMetadataService
import com.netflix.metacat.common.server.util.MetacatContextManager
import com.netflix.spectator.api.NoopRegistry
import spock.lang.Specification

/**
 * Tests for Metadata service.
 * @author amajumdar
 */
class MetadataServiceSpec extends Specification {
    def properties = new MetacatProperties(null)
    def config = new DefaultConfigImpl(properties)
    def registry = new NoopRegistry()
    def tableService = Mock(TableService)
    def partitionService = Mock(PartitionService)
    def mService = Mock(UserMetadataService)
    def tagService = Mock(TagService)
    def aliasService = Mock(AliasService)
    def helper = Mock(MetacatServiceHelper)
    def service = new MetadataService(config, tableService, partitionService, mService, tagService, aliasService,
        helper, registry)
    def name = QualifiedName.ofTable('a','b','c')
    def context = MetacatContextManager.getContext()

    def testDeleteDefinitionMetadata() {
        when:
        service.deleteDefinitionMetadata(name, false, context)
        then:
        1 * helper.getService(name) >> tableService
        1 * tableService.get(name)
        1 * mService.deleteDefinitionMetadata([name])
        when:
        service.deleteDefinitionMetadata(name, true, context)
        then:
        1 * helper.getService(name) >> tableService
        0 * tableService.get(name)
        1 * mService.deleteDefinitionMetadata([name])
    }

    def "rename definition metadata moves metadata and tags and posts events"() {
        given:
        def newName = QualifiedName.ofTable('a', 'b', 'd')
        def oldTable = new TableDto(name: newName)
        def currentTable = new TableDto(name: newName)
        mService.getDefinitionMetadata(name) >> Optional.of(JsonNodeFactory.instance.objectNode())
        tableService.get(newName) >>> [oldTable, currentTable]

        when:
        service.renameDefinitionMetadata(name, newName)

        then:
        1 * helper.postPreRenameTableEvent(name, context, newName)
        1 * mService.renameDefinitionMetadataKey(name, newName)
        1 * tagService.renameTableTags(name, 'd')
        1 * helper.postPostRenameTableEvent(name, context, oldTable, currentTable)
        0 * tableService.rename(_, _, _)
    }

    def "rename definition metadata fails when the new name is an alias"() {
        given:
        def newName = QualifiedName.ofTable('a', 'b', 'd')
        properties.aliasServiceProperties.enabled = true
        aliasService.isAlias(newName) >> true

        when:
        service.renameDefinitionMetadata(name, newName)

        then:
        thrown(MetacatAlreadyExistsException)
        0 * helper.postPreRenameTableEvent(_, _, _)
        0 * mService.renameDefinitionMetadataKey(_, _)
        0 * tagService.renameTableTags(_, _)
    }

    def testDeleteObsoleteDefinitionMetadata() {
        when:
        service.cleanUpObsoleteDefinitionMetadata()
        then:
        1 * mService.searchDefinitionMetadata(_,_,_,_,_,_,_,_) >> [new DefinitionMetadataDto(name: name)]
        1 * helper.getService(name) >> tableService
        1 * tableService.get(name)
        1 * mService.deleteDefinitionMetadata([name])
        when:
        service.cleanUpObsoleteDefinitionMetadata()
        then:
        2 * mService.searchDefinitionMetadata(_,_,_,_,_,_,_,_) >>> [[new DefinitionMetadataDto(name: name)] * 10000, [new DefinitionMetadataDto(name: name)]]
        helper.getService(name) >> tableService
        10001 * mService.deleteDefinitionMetadata(_)
    }
}
