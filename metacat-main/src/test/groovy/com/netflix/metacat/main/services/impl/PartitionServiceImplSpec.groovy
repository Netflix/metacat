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

import com.netflix.metacat.common.dto.GetPartitionsRequestDto
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.converter.DozerTypeConverter
import com.netflix.metacat.common.server.converter.TypeConverterFactory
import com.netflix.metacat.common.server.events.MetacatEventBus
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.metacat.common.server.usermetadata.UserMetadataService
import com.netflix.metacat.common.server.util.ThreadServiceManager
import com.netflix.metacat.main.manager.ConnectorManager
import com.netflix.metacat.main.services.CatalogService
import com.netflix.metacat.main.services.TableService
import com.netflix.metacat.testdata.provider.DataDtoProvider
import com.netflix.metacat.testdata.provider.MetacatDataInfoProvider
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.util.Lists
import spock.lang.Shared
import spock.lang.Specification

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
    def converterUtil = new ConverterUtil(
        new DozerTypeConverter(
            new TypeConverterFactory(
                new DefaultConfigImpl(
                    new MetacatProperties()
                )
            )
        )
    )
    def registry = new SimpleMeterRegistry()
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
}
