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
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.connector.hive.HiveConnectorDatabaseService
import com.netflix.metacat.connector.hive.HiveConnectorTableService
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient
import com.netflix.metacat.connector.hive.commonview.CommonViewHandler
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler
import com.netflix.metacat.connector.hive.util.HiveConfigConstants
import com.netflix.metacat.testdata.provider.DataDtoProvider
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.metastore.api.Table
import spock.lang.Specification

import java.sql.Connection

class HiveConnectorFastTableServiceSpec extends Specification {
    MetacatHiveClient metacatHiveClient = Mock(MetacatHiveClient)
    HiveConnectorDatabaseService hiveConnectorDatabaseService = Mock(HiveConnectorDatabaseService)
    ConnectorRequestContext connectorRequestContext = new ConnectorRequestContext(timestamp:1)
    ConnectorContext connectorContext = DataDtoProvider.newContext(null, ImmutableMap.of(HiveConfigConstants.ALLOW_RENAME_TABLE, "true"))
    IcebergTableHandler handler = Mock(IcebergTableHandler)
    DirectSqlTable directSqlTable = Mock(DirectSqlTable)
    HiveConnectorInfoConverter infoConverter = new HiveConnectorInfoConverter(new HiveTypeConverter())
    CommonViewHandler commonViewHandler = new CommonViewHandler(connectorContext)
    Connection connection = Mock(Connection)
    HiveConnectorTableService service = new HiveConnectorFastTableService(
        "testhive",
        metacatHiveClient,
        hiveConnectorDatabaseService,
        infoConverter,
        connectorContext,
        directSqlTable,
        handler,
        new CommonViewHandler(connectorContext),
        new HiveConnectorFastTableServiceProxy(infoConverter, handler, commonViewHandler)
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

    def "Test get-connection"() {
        given:
        directSqlTable.getConnection() >> connection

        when:
        def actualConnection = service.getConnection()

        then:
        actualConnection == connection
    }
}
