package com.netflix.metacat.main.services

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.DefinitionMetadataDto
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
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
    def config = new DefaultConfigImpl(new MetacatProperties(null))
    def registry = new NoopRegistry()
    def tableService = Mock(TableService)
    def partitionService = Mock(PartitionService)
    def mService = Mock(UserMetadataService)
    def tagService = Mock(TagService)
    def helper = Mock(MetacatServiceHelper)
    def service = new MetadataService(config, tableService, partitionService, mService, tagService, helper, registry)
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
