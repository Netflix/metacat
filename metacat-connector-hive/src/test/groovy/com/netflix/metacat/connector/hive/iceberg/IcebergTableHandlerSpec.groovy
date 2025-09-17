package com.netflix.metacat.connector.hive.iceberg

import com.google.common.collect.ImmutableMap
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException
import com.netflix.metacat.common.server.connectors.exception.TablePreconditionFailedException
import com.netflix.metacat.common.server.connectors.exception.UnsupportedClientOperationException
import com.netflix.metacat.common.server.connectors.model.AuditInfo
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.connector.hive.configs.HiveConnectorFastServiceConfig
import com.netflix.metacat.connector.hive.sql.DirectSqlTable
import com.netflix.metacat.connector.hive.util.HiveConfigConstants
import com.netflix.metacat.testdata.provider.DataDtoProvider
import org.apache.iceberg.ScanSummary
import org.apache.iceberg.Table
import spock.lang.Specification

class IcebergTableHandlerSpec extends Specification{
    def catalogName = 'c'
    def databaseName = 'd'
    def tableName = 't'
    def qualifiedName = QualifiedName.ofTable(catalogName, databaseName, tableName)

    ConnectorRequestContext connectorRequestContext = new ConnectorRequestContext(timestamp:1)
    ConnectorContext connectorContext = DataDtoProvider.newContext(null, ImmutableMap.of(HiveConfigConstants.ALLOW_RENAME_TABLE, "true"))

    IcebergTableHandler icebergTableHandler =
        Spy(new IcebergTableHandler(connectorContext, null, null, new HiveConnectorFastServiceConfig().icebergTableOps()))
    DirectSqlTable directSqlTable = Mock(DirectSqlTable)

    def "Test iceberg table handleUpdate"() {
        given:
        def table = new TableInfo(name: qualifiedName, metadata: ['table_type': 'ICEBERG',
                                                                  'metadata_location':'s3:/c/d/t1',
                                                                  'previous_metadata_location':'s3:/c/d/t'])
        when:
        icebergTableHandler.handleUpdate(connectorRequestContext, directSqlTable, table)
        then:
        noExceptionThrown()
        when:
        icebergTableHandler.handleUpdate(connectorRequestContext, directSqlTable, table)
        then:
        thrown(TableNotFoundException)
        directSqlTable.updateIcebergTable(_) >> { throw new TableNotFoundException(qualifiedName)}
        when:
        icebergTableHandler.handleUpdate(connectorRequestContext, directSqlTable, table)
        then:
        noExceptionThrown()
        icebergTableHandler.update(_) >> true
        when:
        icebergTableHandler.handleUpdate(connectorRequestContext, directSqlTable, table)
        then:
        thrown(TablePreconditionFailedException)
        directSqlTable.updateIcebergTable(_) >> { throw new TablePreconditionFailedException(qualifiedName, '', 'x', 'p')}
        icebergTableHandler.update(_) >> true
    }

    def "test get partitions"() {
        given:
        def tableInfo = new TableInfo(
            name: qualifiedName,
            metadata: [
                'table_type': 'ICEBERG',
                'metadata_location':'s3:/c/d/t1',
                'previous_metadata_location':'s3:/c/d/t'
            ],
            audit: new AuditInfo(createdBy: "ssarma")
        )

        def icebergTableWrapper = Mock(IcebergTableWrapper)
        def icebergTable = Mock(Table)

        icebergTableWrapper.getTable() >> icebergTable

        icebergTableHandler.getIcebergTable(qualifiedName, "s3:/c/d/t1", false) >> icebergTableWrapper

        icebergTableHandler.getIcebergTablePartitionMap(qualifiedName, "filter", icebergTable) >> [
            "p=1": new ScanSummary.PartitionMetrics(fileCount: 1, dataTimestampMillis: 1),
            "p=2": new ScanSummary.PartitionMetrics(fileCount: 2, dataTimestampMillis: 2),
            "p=3": new ScanSummary.PartitionMetrics(fileCount: 3, dataTimestampMillis: 3),
            "p=4": new ScanSummary.PartitionMetrics(fileCount: 4, dataTimestampMillis: 4),
            "p=5": new ScanSummary.PartitionMetrics(fileCount: 5, dataTimestampMillis: 5)
        ]

        when:
        def partitions = icebergTableHandler.getPartitions(
            tableInfo, connectorContext, "filter", partitionIds as List<String>, sort as Sort
        )

        then:
        partitions.collect {it -> it.getName().getPartitionName()} == expected

        where:
        partitionIds   | sort                                    || expected
        ["p=4", "p=1"] | new Sort("dateCreated", SortOrder.ASC)  || ["p=1", "p=4"]
        ["p=1", "p=3"] | new Sort("dateCreated", SortOrder.DESC) || ["p=3", "p=1"]
        ["p=3", "p=1"] | null                                    || ["p=1", "p=3"]
        null           | null                                    || ["p=1", "p=2", "p=3", "p=4", "p=5"]
        []             | null                                    || []
    }

    def "test Netflix Iceberg version validation through public API"() {
        given:
        def mockTableInfo = new TableInfo(
            name: qualifiedName, 
            metadata: [
                'table_type': 'ICEBERG',
                'metadata_location': 's3://test-bucket/metadata.json'
            ]
        )
        def mockRequestContext = new ConnectorRequestContext(timestamp: 1)
        mockRequestContext.additionalContext = ["X-Netflix-Iceberg-Version": versionString] as Map<String, String>

        // Mock the iceberg table wrapper to indicate it has branches/tags
        def mockWrapper = Mock(IcebergTableWrapper) {
            hasBranchesOrTags() >> true
            getBranches() >> ["main", "feature"]
            getTags() >> ["v1.0", "v2.0"]
        }

        when:
        def exceptionThrown = false
        try {
            icebergTableHandler.validateIcebergBranchesTagsSupport(mockRequestContext, qualifiedName, mockTableInfo)
        } catch (UnsupportedClientOperationException e) {
            exceptionThrown = true
        }

        then:
        1 * icebergTableHandler.getIcebergTable(_, _, _) >> mockWrapper
        exceptionThrown == shouldThrowException

        where:
        versionString || shouldThrowException
        // Supported Netflix Iceberg versions (1.1+) - should NOT throw exception
        "1.1.0"      || false
        "1.2.5"      || false
        "1.10.1"     || false
        "2.0.0"      || false
        "3.1.0"      || false

        // Unsupported Netflix Iceberg versions (< 1.1) - should throw exception
        "0.14.1"     || true
        "1.0.0"      || true
        "1.0.9"      || true

        // Invalid version formats - should throw exception
        "invalid"    || true
        "1"          || true
        ""           || true
    }
}
