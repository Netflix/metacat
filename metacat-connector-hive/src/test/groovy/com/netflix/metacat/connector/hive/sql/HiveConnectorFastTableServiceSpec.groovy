/*
 *  Copyright 2019 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive.sql

import com.google.common.collect.ImmutableMap
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException
import com.netflix.metacat.common.server.connectors.exception.TablePreconditionFailedException
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.connector.hive.HiveConnectorDatabaseService
import com.netflix.metacat.connector.hive.HiveConnectorTableService
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient
import com.netflix.metacat.connector.hive.commonview.CommonViewHandler
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler
import com.netflix.metacat.connector.hive.util.HiveConfigConstants
import com.netflix.spectator.api.Registry
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.metastore.api.Table
import spock.lang.Specification

class HiveConnectorFastTableServiceSpec extends Specification {
    MetacatHiveClient metacatHiveClient = Mock(MetacatHiveClient)
    HiveConnectorDatabaseService hiveConnectorDatabaseService = Mock(HiveConnectorDatabaseService)
    ConnectorRequestContext connectorRequestContext = new ConnectorRequestContext(timestamp:1)
    ConnectorContext connectorContext = new ConnectorContext(
        "testHive",
        "testHive",
        "hive",
        Mock(Config),
        Mock(Registry),
        ImmutableMap.of(HiveConfigConstants.ALLOW_RENAME_TABLE, "true"))
    IcebergTableHandler handler = Mock(IcebergTableHandler)
    DirectSqlTable directSqlTable = Mock(DirectSqlTable)
    HiveConnectorTableService service = new HiveConnectorFastTableService(
        "testhive",
        metacatHiveClient,
        hiveConnectorDatabaseService,
        new HiveConnectorInfoConverter(new HiveTypeConverter()),
        connectorContext,
        directSqlTable,
        handler,
        new CommonViewHandler(connectorContext)
    )
    def catalogName = 'c'
    def databaseName = 'd'
    def tableName = 't'
    def qualifiedName = QualifiedName.ofTable(catalogName, databaseName, tableName)

    def "Test non-iceberg table update"() {
        given:
        def table = new TableInfo(name: qualifiedName, metadata: ['metadata_location':'s3:/c/d/t1'])
        when:
        service.update(connectorRequestContext, table)
        then:
        noExceptionThrown()
        metacatHiveClient.getTableByName(_,_) >> new Table(tableName:tableName, dbName: databaseName,
            sd: new StorageDescriptor(cols: [new FieldSchema('a', 'int', 'comment')],
                serdeInfo: new SerDeInfo(serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'), outputFormat: 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'),
            partitionKeys: [])
        when:
        service.update(connectorRequestContext, table)
        then:
        thrown(TableNotFoundException)
        metacatHiveClient.getTableByName(_,_) >> { throw new NoSuchObjectException()}
    }

    def "Test iceberg table update"() {
        given:
        def table = new TableInfo(name: qualifiedName, metadata: ['table_type': 'ICEBERG','metadata_location':'s3:/c/d/t1', 'previous_metadata_location':'s3:/c/d/t'])
        when:
        service.update(connectorRequestContext, table)
        then:
        noExceptionThrown()
        when:
        service.update(connectorRequestContext, table)
        then:
        thrown(TableNotFoundException)
        directSqlTable.updateIcebergTable(_) >> { throw new TableNotFoundException(qualifiedName)}
        when:
        service.update(connectorRequestContext, table)
        then:
        noExceptionThrown()
        handler.update(_) >> true
        when:
        service.update(connectorRequestContext, table)
        then:
        thrown(TablePreconditionFailedException)
        directSqlTable.updateIcebergTable(_) >> { throw new TablePreconditionFailedException(qualifiedName, '', 'x', 'p')}
        handler.update(_) >> true
    }
}
