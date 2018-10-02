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
import com.netflix.metacat.common.dto.AuditDto
import com.netflix.metacat.common.dto.StorageDto
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.ConnectorTableService
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.events.MetacatEventBus
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.usermetadata.DefaultAuthorizationService
import com.netflix.metacat.common.server.usermetadata.TagService
import com.netflix.metacat.common.server.usermetadata.UserMetadataService
import com.netflix.metacat.main.manager.ConnectorManager
import com.netflix.metacat.main.services.DatabaseService
import com.netflix.metacat.main.services.GetTableServiceParameters
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
    def connectorTableServiceProxy
    def authorizationService

    def service
    def setup() {
        config.getMetacatCreateAcl() >> new HashMap<QualifiedName, Set<String>>()
        config.getMetacatDeleteAcl() >> new HashMap<QualifiedName, Set<String>>()
        config.isAuthorizationEnabled() >> true
        connectorManager.getTableService(_) >> connectorTableService
        converterUtil.toTableDto(_) >> tableDto
        converterUtil.toConnectorContext(_) >> Mock(ConnectorRequestContext)
        usermetadataService.getDefinitionMetadata(_) >> Optional.empty()
        usermetadataService.getDataMetadata(_) >> Optional.empty()
        usermetadataService.getDefinitionMetadataWithInterceptor(_,_) >> Optional.empty()
        connectorTableServiceProxy = new ConnectorTableServiceProxy(connectorManager, converterUtil)
        authorizationService = new DefaultAuthorizationService(config)
        service = new TableServiceImpl(connectorTableServiceProxy, databaseService, tagService,
            usermetadataService, eventBus, registry, config, authorizationService)
    }

    def testTableGet() {
        when:
        service.get(name,GetTableServiceParameters.builder().
            disableOnReadMetadataIntercetor(false)
            .includeDataMetadata(true)
            .includeDefinitionMetadata(true)
            .build())
        then:
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
        1 * usermetadataService.getDefinitionMetadata(_) >> Optional.empty()
        1 * usermetadataService.getDataMetadata(_) >> Optional.empty()
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
