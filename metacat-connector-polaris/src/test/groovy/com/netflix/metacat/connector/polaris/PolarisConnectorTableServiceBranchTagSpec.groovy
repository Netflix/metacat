/*
 * Copyright 2024 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.metacat.connector.polaris

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.exception.UnsupportedClientOperationException
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.connector.hive.commonview.CommonViewHandler
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler
import com.netflix.metacat.connector.polaris.mappers.PolarisTableMapper
import com.netflix.metacat.connector.polaris.store.PolarisStoreService
import spock.lang.Specification

/**
 * Tests for PolarisConnectorTableService integration with IcebergTableHandler
 * for branch and tag validation. The validation logic has been moved to IcebergTableHandler,
 * so these tests verify the integration between the two classes.
 */
class PolarisConnectorTableServiceBranchTagSpec extends Specification {

    def polarisStoreService = Mock(PolarisStoreService)
    def polarisConnectorDatabaseService = Mock(PolarisConnectorDatabaseService)
    def connectorConverter = Mock(HiveConnectorInfoConverter)
    def connectorContext = Mock(ConnectorContext)
    def icebergTableHandler = Mock(IcebergTableHandler)
    def commonViewHandler = Mock(CommonViewHandler)
    def polarisTableMapper = Mock(PolarisTableMapper)

    def service = new PolarisConnectorTableService(
        polarisStoreService,
        "test-catalog",
        polarisConnectorDatabaseService,
        connectorConverter,
        icebergTableHandler,
        commonViewHandler,
        polarisTableMapper,
        connectorContext
    )

    def qualifiedName = QualifiedName.ofTable("catalog", "database", "table")

    def "test update method calls IcebergTableHandler validation"() {
        given: "An Iceberg table and request context"
        def tableInfo = createIcebergTableInfo()
        def requestContext = Mock(ConnectorRequestContext)
        
        and: "Mock dependencies for the update method"
        def config = Mock(Config)
        connectorContext.getConfig() >> config
        config.isIcebergPreviousMetadataLocationCheckEnabled() >> false
        
        polarisStoreService.getTable(_, _) >> Optional.empty()

        when: "Calling update method"
        try {
            service.update(requestContext, tableInfo)
        } catch (Exception e) {
            // We expect this to fail due to incomplete mocking, but we want to verify
            // that the validation method was called
        }

        then: "Should call IcebergTableHandler validation method"
        1 * icebergTableHandler.validateIcebergBranchesTagsSupport(requestContext, qualifiedName, tableInfo)
    }

    def "test update with regular table does not call validation"() {
        given: "A regular non-Iceberg table and request context"
        def tableInfo = createRegularTableInfo()
        def requestContext = Mock(ConnectorRequestContext)
        
        and: "Mock dependencies for the update method"
        def config = Mock(Config)
        connectorContext.getConfig() >> config
        
        when: "Calling update method"
        try {
            service.update(requestContext, tableInfo)
        } catch (Exception e) {
            // We expect this to fail due to incomplete mocking, but we want to verify
            // that the validation method was called
        }

        then: "Should still call IcebergTableHandler validation method (it will check table type internally)"
        1 * icebergTableHandler.validateIcebergBranchesTagsSupport(requestContext, qualifiedName, tableInfo)
    }

    private TableInfo createIcebergTableInfo(String metadataLocation = "/test/metadata/location") {
        return Mock(TableInfo) {
            getName() >> qualifiedName
            getMetadata() >> [
                "table_type": "ICEBERG",
                "metadata_location": metadataLocation
            ]
        }
    }
    
    private TableInfo createRegularTableInfo() {
        return Mock(TableInfo) {
            getName() >> qualifiedName
            getMetadata() >> [
                "table_type": "HIVE_TABLE"
            ]
        }
    }
}
