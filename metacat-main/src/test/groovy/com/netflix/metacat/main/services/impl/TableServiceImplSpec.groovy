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

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorTableService
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.events.MetacatEventBus
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.usermetadata.TagService
import com.netflix.metacat.common.server.usermetadata.UserMetadataService
import com.netflix.metacat.main.manager.ConnectorManager
import com.netflix.metacat.main.services.DatabaseService
import com.netflix.metacat.main.services.TableService
import com.netflix.metacat.testdata.provider.DataDtoProvider
import com.netflix.spectator.api.NoopRegistry
import spock.lang.Specification

/**
 * Tests for the TableServiceImpl.
 *
 * @author amajumdar
 * @since 1.2.0
 */
class TableServiceImplSpec extends Specification {
    def connectorManager = Mock(ConnectorManager)
    def connectorTableService = Mock(ConnectorTableService)
    def databaseService = Mock(DatabaseService)
    def tagService = Mock(TagService)
    def usermetadataService = Mock(UserMetadataService)
    def eventBus = Mock(MetacatEventBus)
    def converterUtil = Mock(ConverterUtil)
    def registry = new NoopRegistry()
    def config = Mock(Config)
    def tableDto = DataDtoProvider.getTable('a', 'b', 'c', "amajumdar", "s3:/a/b")
    def name = tableDto.name
    TableService service
    def setup() {
        connectorManager.getTableService(_) >> connectorTableService
        converterUtil.toTableDto(_) >> tableDto
        usermetadataService.getDefinitionMetadata(_) >> Optional.empty()
        usermetadataService.getDataMetadata(_) >> Optional.empty()
        service = new TableServiceImpl(connectorManager, databaseService, tagService, usermetadataService, eventBus, converterUtil, registry, config)
    }
    def testDeleteAndReturn() {
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.canDeleteTableDefinitionMetadata() >> true
        1 * usermetadataService.deleteMetadata(_,_)
        0 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.canDeleteTableDefinitionMetadata() >> false
        1 * config.getNamesEnabledForDefinitionMetadataDelete() >> [QualifiedName.fromString('a')]
        1 * usermetadataService.deleteMetadata(_,_)
        0 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.canDeleteTableDefinitionMetadata() >> false
        1 * config.getNamesEnabledForDefinitionMetadataDelete() >> [QualifiedName.fromString('a/b')]
        1 * usermetadataService.deleteMetadata(_,_)
        0 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.canDeleteTableDefinitionMetadata() >> false
        1 * config.getNamesEnabledForDefinitionMetadataDelete() >> [QualifiedName.fromString('a/b/c')]
        1 * usermetadataService.deleteMetadata(_,_)
        0 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.canDeleteTableDefinitionMetadata() >> false
        1 * config.getNamesEnabledForDefinitionMetadataDelete() >> []
        1 * config.canSoftDeleteDataMetadata() >> true
        0 * usermetadataService.deleteMetadata(_,_)
        1 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
        when:
        service.deleteAndReturn(name, false)
        then:
        1 * config.canDeleteTableDefinitionMetadata() >> false
        1 * config.getNamesEnabledForDefinitionMetadataDelete() >> [QualifiedName.fromString('a/c')]
        1 * config.canSoftDeleteDataMetadata() >> true
        0 * usermetadataService.deleteMetadata(_,_)
        1 * usermetadataService.softDeleteDataMetadata(_,_)
        noExceptionThrown()
    }
}
