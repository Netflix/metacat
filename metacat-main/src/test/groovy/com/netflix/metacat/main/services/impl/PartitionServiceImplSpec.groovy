/*
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.metacat.main.services.impl

import com.google.common.collect.Sets
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.GetPartitionsRequestDto
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService
import com.netflix.metacat.common.server.connectors.exception.TableMigrationInProgressException
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveResponse
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.converter.DefaultTypeConverter
import com.netflix.metacat.common.server.converter.DozerJsonTypeConverter
import com.netflix.metacat.common.server.converter.DozerTypeConverter
import com.netflix.metacat.common.server.converter.TypeConverterFactory
import com.netflix.metacat.common.server.events.MetacatEventBus
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.metacat.common.server.usermetadata.UserMetadataService
import com.netflix.metacat.common.server.util.MetacatUtils
import com.netflix.metacat.common.server.util.ThreadServiceManager
import com.netflix.metacat.main.manager.ConnectorManager
import com.netflix.metacat.main.services.CatalogService
import com.netflix.metacat.main.services.TableService
import com.netflix.metacat.testdata.provider.DataDtoProvider
import com.netflix.metacat.testdata.provider.MetacatDataInfoProvider
import com.netflix.metacat.testdata.provider.PigDataDtoProvider
import com.netflix.spectator.api.NoopRegistry
import org.assertj.core.util.Lists
import spock.lang.Shared
import spock.lang.Specification

import java.util.stream.Collectors

/**
 * Tests for the PartitionServiceImpl.
 *
 * @author zhenl
 * @since 1.2.0
 */
class PartitionServiceImplSpec extends Specification{
    def catalogService = Mock(CatalogService)
    def connectorManager = Mock(ConnectorManager)
    def connectorPartitionService = Mock(ConnectorPartitionService)
    def tableService = Mock(TableService)
    def usermetadataService = Mock(UserMetadataService)
    def eventBus = Mock(MetacatEventBus)
    @Shared
    def typeFactory = new TypeConverterFactory(new DefaultTypeConverter())
    @Shared
    def converterUtil = new ConverterUtil(new DozerTypeConverter(typeFactory), new DozerJsonTypeConverter(typeFactory))
    def registry = new NoopRegistry()
    def config = Mock(Config)
    def threadServiceManager = Mock(ThreadServiceManager)
    def tableDto = DataDtoProvider.getTable('a', 'b', 'c', "amajumdar", "s3:/a/b")
    def name = tableDto.name

    def setup() {
        connectorManager.getPartitionService(_) >> connectorPartitionService
        usermetadataService.getDefinitionMetadata(_) >> Optional.empty()
        usermetadataService.getDataMetadata(_) >> Optional.empty()
        connectorPartitionService.getPartitions(_,_,_) >> Lists.emptyList()
        config.getNamesToThrowErrorOnListPartitionsWithNoFilter() >> Lists.emptyList()
        tableService.get(_,_) >> Optional.of(TableDto.builder().build())
    }
    /**
     * Test the null pointer cases in getPartitionRequestDto
     */
    def testlistpartitions() {
        when:
        def service = new PartitionServiceImpl(catalogService, connectorManager, tableService, usermetadataService,
            threadServiceManager, config, eventBus, converterUtil, registry)
        service.list(name, null, null, false, false, null)
        then:
        noExceptionThrown()

        when:
        service = new PartitionServiceImpl(catalogService, connectorManager, tableService, usermetadataService,
            threadServiceManager, config, eventBus, converterUtil, registry)
        service.list(name, null, null, false, false,
        new GetPartitionsRequestDto(null, null, false, false))
        then:
        noExceptionThrown()

        when:
        service = new PartitionServiceImpl(catalogService, connectorManager, tableService, usermetadataService,
            threadServiceManager, config, eventBus, converterUtil, registry)
        service.list(name, null, null, false, false,
            new GetPartitionsRequestDto())
        then:
        noExceptionThrown()

    }

    def "Test Save/Delete Partitions Handling When Hive To Iceberg Migration In Progress"() {
        given:
        def testTable = QualifiedName.fromString("prodhive/account/test")
        def tagSet = Sets.newHashSet(MetacatUtils.ICEBERG_MIGRATION_DO_NOT_MODIFY_TAG)
        def tableDtoWithTags = DataDtoProvider.getTable(testTable.getCatalogName(), testTable.getDatabaseName(), testTable.getTableName(),
                "warehouse", "s3://prodhive/account/test", tagSet)
        def partitionService = new PartitionServiceImpl(catalogService, connectorManager, tableService, usermetadataService,
                threadServiceManager, config, eventBus, converterUtil, registry)
        def partitions = PigDataDtoProvider.getPartitions(testTable.getCatalogName(), testTable.getDatabaseName(), testTable.getTableName(),
                'field1=xyz/field3=abc', "s3://prodhive/account/test", 5)

        when:
        partitionService.save(tableDtoWithTags.getName(), new PartitionsSaveRequestDto(partitions: partitions))

        then:
        1 * tableService.exists(testTable) >> true
        2 * config.getNoTableUpdateOnTags() >> Sets.newHashSet(tagSet)
        1 * tableService.get(testTable, _) >> Optional.of(tableDtoWithTags)
        thrown(TableMigrationInProgressException)

        when:
        partitionService.delete(tableDtoWithTags.getName(), partitions.stream().map({ p -> p.name }).collect(Collectors.toList()))

        then:
        1 * config.getMaxDeletedPartitionsThreshold() >> 1000
        1 * tableService.exists(testTable) >> true
        2 * config.getNoTableDeleteOnTags() >> Sets.newHashSet(tagSet)
        1 * tableService.get(testTable, _) >> Optional.of(tableDtoWithTags)
        thrown(TableMigrationInProgressException)

        when:
        partitionService.save(tableDtoWithTags.getName(), new PartitionsSaveRequestDto(partitions: partitions))

        then:
        1 * tableService.exists(testTable) >> true
        1 * config.getNoTableUpdateOnTags() >> Sets.newHashSet("do_not_rename", "uncategorized")
        1 * config.getMaxAddedPartitionsThreshold() >> 1000
        1 * connectorPartitionService.savePartitions(_, _, _) >> new PartitionsSaveResponse()
        noExceptionThrown()
    }
}
