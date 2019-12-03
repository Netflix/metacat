package com.netflix.metacat.connector.hive.iceberg

import com.google.common.collect.ImmutableMap
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException
import com.netflix.metacat.common.server.connectors.exception.TablePreconditionFailedException
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.connector.hive.sql.DirectSqlTable
import com.netflix.metacat.connector.hive.util.HiveConfigConstants
import com.netflix.spectator.api.Registry
import spock.lang.Specification

class IcebergTableHandlerSpec extends Specification{
    def catalogName = 'c'
    def databaseName = 'd'
    def tableName = 't'
    def qualifiedName = QualifiedName.ofTable(catalogName, databaseName, tableName)

    ConnectorRequestContext connectorRequestContext = new ConnectorRequestContext(timestamp:1)
    ConnectorContext connectorContext = new ConnectorContext(
        "testHive",
        "testHive",
        "hive",
        Mock(Config),
        Mock(Registry),
        ImmutableMap.of(HiveConfigConstants.ALLOW_RENAME_TABLE, "true"))

    IcebergTableHandler icebergTableHandler =
        new IcebergTableHandler(connectorContext, null, null)
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
}
