/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.thrift

import com.facebook.presto.hive.$internal.com.facebook.fb303.fb_status
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.netflix.metacat.common.MetacatContext
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.api.MetacatV1
import com.netflix.metacat.common.api.PartitionV1
import com.netflix.metacat.common.dto.*
import com.netflix.metacat.common.server.Config
import com.netflix.metacat.common.util.MetacatContextManager
import com.netflix.metacat.converters.HiveConverters
import com.netflix.metacat.converters.TypeConverterProvider
import org.apache.hadoop.hive.metastore.api.*
import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class CatalogThriftHiveMetastoreSpec extends Specification {
    TypeConverterProvider typeConverterProvider = {
        def mock = Mock(TypeConverterProvider)
        mock.defaultConverterType >> MetacatContext.DATA_TYPE_CONTEXTS.pig

        return mock
    }()
    Config config = Mock(Config)
    HiveConverters hiveConverters = Mock(HiveConverters)
    MetacatV1 metacatV1 = Mock(MetacatV1)
    PartitionV1 partitionV1 = Mock(PartitionV1)
    String catalogName = 'testCatalogName'
    CatalogThriftEventHandler.CatalogServerContext catalogServerContext = Mock(CatalogThriftEventHandler.CatalogServerContext)
    CatalogThriftHiveMetastore ms = new CatalogThriftHiveMetastore(
            config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName)

    def setup() {
        MetacatContextManager.context = catalogServerContext
    }

    def 'test abort_txn'() {
        when:
        ms.abort_txn(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test add_dynamic_partitions'() {
        when:
        ms.add_dynamic_partitions(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test add_index'() {
        when:
        ms.add_index(null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test add_partition'() {
        when:
        def result = ms.add_partition(partition)

        then:
        notThrown(Exception)
        result == partition

        where:
        partition = new Partition(dbName: 'db1', tableName: 't1')
    }

    def 'test add_partition_with_environment_context'() {
        when:
        def result = ms.add_partition_with_environment_context(partition, null)

        then:
        notThrown(Exception)
        result == partition

        where:
        partition = new Partition(dbName: 'db1', tableName: 't1')
    }

    def 'test add_partitions'() {
        when:
        def result = ms.add_partitions(partitions)

        then:
        notThrown(Exception)
        result == partitions.size()

        where:
        partitions << [[new Partition(dbName: 'db1', tableName: 't1')], []]
    }

    def 'test add_partitions_pspec'() {
        when:
        ms.add_partitions_pspec(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test add_partitions_req'() {
        when:
        def result = ms.add_partitions_req(request)

        then:
        notThrown(Exception)
        result.partitions?.size() == request.parts.size()

        where:
        request << [
                new AddPartitionsRequest('db1', 't1', [new Partition(dbName: 'db1', tableName: 't1')], true)
        ]
    }

    def 'test alter_database'() {
        when:
        ms.alter_database(null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test alter_function'() {
        when:
        ms.alter_function(null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test alter_index'() {
        when:
        ms.alter_index(null, null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test alter_partition'() {
        when:
        ms.alter_partition(db, tbl, partition)

        then:
        notThrown(Exception)

        where:
        db = 'db1'
        tbl = 't1'
        partition = new Partition(dbName: db, tableName: tbl)
    }

    def 'test alter_partition_with_environment_context'() {
        when:
        ms.alter_partition_with_environment_context(db, tbl, partition, null)

        then:
        notThrown(Exception)

        where:
        db = 'db1'
        tbl = 't1'
        partition = new Partition(dbName: db, tableName: tbl)
    }

    def 'test alter_partitions'() {
        when:
        ms.alter_partitions(db, tbl, [partition])

        then:
        notThrown(Exception)

        where:
        db = 'db1'
        tbl = 't1'
        partition = new Partition(dbName: db, tableName: tbl)
    }

    def 'test alter_table'() {
        when:
        ms.alter_table(db, tbl, hiveTable)

        then:
        notThrown(Exception)
        if (tbl != new_name) {
            1 * metacatV1.renameTable(_, _, _, _)
        } else {
            0 * metacatV1.renameTable(_, _, _, _)
        }
        1 * metacatV1.updateTable(_, _, _, _)

        where:
        db = 'db1'
        tbl = 't1'
        new_name << ['t1', 't2']
        hiveTable = new Table(dbName: db, tableName: new_name)
    }

    def 'test alter_table_with_cascade'() {
        when:
        ms.alter_table_with_cascade(db, tbl, hiveTable, false)

        then:
        notThrown(Exception)
        if (tbl != new_name) {
            1 * metacatV1.renameTable(_, _, _, _)
        } else {
            0 * metacatV1.renameTable(_, _, _, _)
        }
        1 * metacatV1.updateTable(_, _, _, _)

        where:
        db = 'db1'
        tbl = 't1'
        new_name << ['t1', 't2']
        hiveTable = new Table(dbName: db, tableName: new_name)
    }

    def 'test alter_table_with_environment_context'() {
        when:
        ms.alter_table_with_environment_context(db, tbl, hiveTable, ec)

        then:
        notThrown(Exception)
        if (tbl != new_name) {
            1 * metacatV1.renameTable(_, _, _, _)
        } else {
            0 * metacatV1.renameTable(_, _, _, _)
        }
        1 * metacatV1.updateTable(_, _, _, _)

        where:
        db = 'db1'
        tbl = 't1'
        new_name << ['t1', 't2']
        hiveTable = new Table(dbName: db, tableName: new_name)
        ec = new EnvironmentContext()
    }

    def 'test append_partition'() {
        when:
        ms.append_partition(db, tbl, ['20150101'])

        then:
        notThrown(Exception)
        1 * hiveConverters.getNameFromPartVals(_,_) >> 'dateint=20150101'
        1 * metacatV1.getTable(_,_,_,_,_,_) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, _, _,_,_,_, { it.includePartitionDetails }) >>
                [new PartitionDto(name: QualifiedName.ofPartition(catalogName, db, tbl, 'dateint=20150101'))]

        where:
        db = 'db1'
        tbl = 't1'
    }

    def 'test append_partition_by_name'() {
        when:
        ms.append_partition_by_name(db, tbl, 'dateint=20150101')

        then:
        notThrown(Exception)
        1 * metacatV1.getTable(_,_,_,_,_,_) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, _, _,_,_,_, { it.includePartitionDetails }) >>
                [new PartitionDto(name: QualifiedName.ofPartition(catalogName, db, tbl, 'dateint=20150101'))]

        where:
        db = 'db1'
        tbl = 't1'
    }

    def 'test append_partition_by_name_with_environment_context'() {
        when:
        ms.append_partition_by_name_with_environment_context(db, tbl, partName, null)

        then:
        notThrown(Exception)
        1 * metacatV1.getTable(_,_,_,_,_,_) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, _, _,_,_,_, { it.includePartitionDetails }) >>
                [new PartitionDto(name: QualifiedName.ofPartition(catalogName, db, tbl, 'dateint=20150101'))]

        where:
        db = 'db1'
        tbl = 't1'
        partName = 'dateint=20150101'
    }

    def 'test append_partition_with_environment_context'() {
        when:
        ms.append_partition_with_environment_context(db, tbl, ['20150101'], null)

        then:
        notThrown(Exception)
        1 * hiveConverters.getNameFromPartVals(_,_) >> 'dateint=20150101'
        1 * metacatV1.getTable(_,_,_,_,_,_) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, _, _,_,_,_, { it.includePartitionDetails }) >>
                [new PartitionDto(name: QualifiedName.ofPartition(catalogName, db, tbl, 'dateint=20150101'))]
        where:
        db = 'db1'
        tbl = 't1'
    }

    def 'test cancel_delegation_token'() {
        when:
        ms.cancel_delegation_token(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test check_lock'() {
        when:
        ms.check_lock(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test commit_txn'() {
        when:
        ms.commit_txn(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test compact'() {
        when:
        ms.compact(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test create_database'() {
        when:
        ms.create_database(hiveDatabase)

        then:
        1 * metacatV1.createDatabase(_, db, null)

        where:
        db = 'db1'
        tbl = 't1'
        hiveDatabase = new Database(name: db)
    }

    def 'test create_function'() {
        when:
        ms.create_function(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test create_role'() {
        when:
        ms.create_role(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test create_table'() {
        when:
        ms.create_table(hiveTable)

        then:
        notThrown(Exception)
        1 * hiveConverters.hiveToMetacatTable(_, hiveTable)
        1 * metacatV1.createTable(_, _, _, _)

        where:
        db = 'db1'
        tbl = 't1'
        hiveTable = new Table(dbName: db, tableName: tbl)
    }

    def 'test create_table_with_environment_context'() {
        when:
        ms.create_table_with_environment_context(hiveTable, ec)

        then:
        notThrown(Exception)
        1 * hiveConverters.hiveToMetacatTable(_, hiveTable)
        1 * metacatV1.createTable(_, _, _, _)

        where:
        db = 'db1'
        tbl = 't1'
        hiveTable = new Table(dbName: db, tableName: tbl)
        ec = new EnvironmentContext()
    }

    def 'test create_type'() {
        when:
        ms.create_type(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test delete_partition_column_statistics'() {
        when:
        ms.delete_partition_column_statistics(null, null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test delete_table_column_statistics'() {
        when:
        ms.delete_table_column_statistics(null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test drop_database'() {
        when:
        ms.drop_database(null, true, true)

        then:
        thrown(InvalidOperationException)
    }

    def 'test drop_function'() {
        when:
        ms.drop_function(null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test drop_index_by_name'() {
        when:
        ms.drop_index_by_name(null, null, null, true)

        then:
        thrown(InvalidOperationException)
    }

    def 'test drop_partition'() {
        when:
        def r = ms.drop_partition(db, tbl, partVals, true)

        then:
        r
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl),
                fields: (0..10).collect { new FieldDto(name: "field_$it", partition_key: it < 2) }
        )
        1 * hiveConverters.getNameFromPartVals(_,_) >> partName
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, _, null, null, null, false, { it.includePartitionDetails }) >>
                [new PartitionDto(name: QualifiedName.ofPartition(catalogName, db, tbl, partName))]
        1 * partitionV1.deletePartitions(_, db, tbl, [partName])

        where:
        db = 'db1'
        tbl = 't1'
        partVals = ['p0','p1']
        partName = 'field_0=p0/field_1=p1'
    }

    def 'test drop_partition_by_name'() {
        when:
        def r = ms.drop_partition_by_name(db, tbl, partName, true)

        then:
        r
        1 * partitionV1.deletePartitions(_, db, tbl, [partName])

        where:
        db = 'db1'
        tbl = 't1'
        partName = 'p1'
    }

    def 'test drop_partition_by_name_with_environment_context'() {
        when:
        def r = ms.drop_partition_by_name_with_environment_context(db, tbl, partName, true, new EnvironmentContext())

        then:
        r
        1 * partitionV1.deletePartitions(_, db, tbl, [partName])

        where:
        db = 'db1'
        tbl = 't1'
        partName = 'p1'
    }

    def 'test drop_partition_with_environment_context exception not enough'() {
        when:
        def r = ms.drop_partition_with_environment_context(db, tbl, partVals, true, new EnvironmentContext())

        then:
        thrown(NoSuchObjectException)
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl),
                fields: (0..10).collect { new FieldDto(name: "field_$it", partition_key: it < 2) }
        )
        1 * hiveConverters.getNameFromPartVals(_,_) >> partName
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, _, null, null, null, false, { it.includePartitionDetails })
        0 * partitionV1.deletePartitions(_, db, tbl, [partName])

        where:
        db = 'db1'
        tbl = 't1'
        partVals = ['p0','p1']
        partName = 'field_0=p0/field_1=p1'
    }

    def 'test drop_partition_with_environment_context exception too many'() {
        when:
        def r = ms.drop_partition_with_environment_context(db, tbl, partVals, true, new EnvironmentContext())

        then:
        thrown(NoSuchObjectException)
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl),
                fields: (0..10).collect { new FieldDto(name: "field_$it", partition_key: it < 2) }
        )
        1 * hiveConverters.getNameFromPartVals(_,_) >> partName
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, _, null, null, null, false, { it.includePartitionDetails }) >>
                [new PartitionDto(), new PartitionDto()]
        0 * partitionV1.deletePartitions(_, db, tbl, [partName])

        where:
        db = 'db1'
        tbl = 't1'
        partVals = ['p0','p1']
        partName = 'field_0=p0/field_1=p1'
    }

    def 'test drop_partition_with_environment_context'() {
        when:
        def r = ms.drop_partition_with_environment_context(db, tbl, partVals, true, new EnvironmentContext())

        then:
        r
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl),
                fields: (0..10).collect { new FieldDto(name: "field_$it", partition_key: it < 2) }
        )
        1 * hiveConverters.getNameFromPartVals(_,_) >> partName
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, _, null, null, null, false, { it.includePartitionDetails }) >>
                [new PartitionDto(name: QualifiedName.ofPartition(catalogName, db, tbl, partName))]
        1 * partitionV1.deletePartitions(_, db, tbl, [partName])

        where:
        db = 'db1'
        tbl = 't1'
        partVals = ['p0','p1']
        partName = 'field_0=p0/field_1=p1'
    }

    def 'test drop_partitions_req'() {
        when:
        ms.drop_partitions_req(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test drop_role'() {
        when:
        ms.drop_role(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test drop_table'() {
        given:
        metacatV1.deleteTable(_, db, tbl) >> new TableDto()

        when:
        ms.drop_table(db, tbl, drop)

        then:
        notThrown(Exception)

        where:
        db = 'db1'
        tbl = 't1'
        drop << [true, false]
    }

    def 'test drop_table_with_environment_context'() {
        given:
        metacatV1.deleteTable(_, db, tbl) >> new TableDto()

        when:
        ms.drop_table_with_environment_context(db, tbl, drop, ec)

        then:
        notThrown(Exception)

        where:
        db = 'db1'
        tbl = 't1'
        drop << [true, false]
        ec = new EnvironmentContext()
    }

    def 'test drop_type'() {
        when:
        ms.drop_type(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test exchange_partition'() {
        when:
        ms.exchange_partition(null, null, null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test fire_listener_event'() {
        when:
        ms.fire_listener_event(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test getCpuProfile'() {
        expect:
        ms.getCpuProfile(42) == ''
    }

    def 'test getMetaConf'() {
        when:
        ms.getMetaConf(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test getStatus'() {
        expect:
        ms.status == fb_status.ALIVE
    }

    def 'test getVersion'() {
        expect:
        ms.version == '3.0'
    }

    def 'test get_aggr_stats_for'() {
        when:
        ms.get_aggr_stats_for(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_all_databases'() {
        given:
        def databases = ['db1', 'db2', 'db3']
        metacatV1.getCatalog(_) >> new CatalogDto(databases: databases)

        when:
        def result = ms.get_all_databases()

        then:
        result == databases
    }

    def 'test get_all_tables'() {
        given:
        def db = 'db1'
        def tables = ['t1', 't2', 't3']
        metacatV1.getDatabase(_, db, false) >> new DatabaseDto(tables: tables)

        when:
        def result = ms.get_all_tables(db)

        then:
        result == tables
    }

    def 'test get_config_value'() {
        when:
        ms.get_config_value(null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_current_notificationEventId'() {
        when:
        ms.get_current_notificationEventId()

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_database'() {
        when:
        def result = ms.get_database(db)

        then:
        result
        notThrown(Exception)
        1 * metacatV1.getDatabase(_, db, true)
        1 * hiveConverters.metacatToHiveDatabase(_) >> new Database()

        where:
        db = 'db1'
    }

    def 'test get_databases'() {
        given:
        metacatV1.getCatalog(_) >> new CatalogDto(
                databases: ['adb', 'bdb', 'cdb', 'ddb', 'ddb1', 'ddb2', 'ddb3', 'edb', 'fdb']
        )

        when:
        def result = ms.get_databases(pattern)

        then:
        notThrown(Exception)
        result == expectedResult

        // I am guessing on these patterns as I have not been able to find documentation on the format
        where:
        pattern   | expectedResult
        '.*'      | ['adb', 'bdb', 'cdb', 'ddb', 'ddb1', 'ddb2', 'ddb3', 'edb', 'fdb']
        'd.*'     | ['ddb', 'ddb1', 'ddb2', 'ddb3']
        'd.*|ADB' | ['adb', 'ddb', 'ddb1', 'ddb2', 'ddb3']
    }

    def 'test get_delegation_token'() {
        when:
        ms.get_delegation_token(null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_fields'() {
        given:
        def cols = [new FieldSchema(name: 'c1'), new FieldSchema(name: 'c2')]
        def partitionKeys = [new FieldSchema(name: 'pk1'), new FieldSchema(name: 'pk2')]
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            Table get_table(String _dbname, String _tbl_name) throws MetaException {
                return new Table(
                        partitionKeys: partitionKeys,
                        sd: new StorageDescriptor(
                                cols: cols
                        )
                )
            }
        }

        when:
        def result = ms.get_fields(db, tbl)

        then:
        notThrown(Exception)
        result == cols

        where:
        db = 'db1'
        tbl = 't1'
    }

    def 'test get_fields_with_environment_context'() {
        given:
        def cols = [new FieldSchema(name: 'c1'), new FieldSchema(name: 'c2')]
        def partitionKeys = [new FieldSchema(name: 'pk1'), new FieldSchema(name: 'pk2')]
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            Table get_table(String _dbname, String _tbl_name) throws MetaException {
                return new Table(
                        partitionKeys: partitionKeys,
                        sd: new StorageDescriptor(
                                cols: cols
                        )
                )
            }
        }

        when:
        def result = ms.get_fields_with_environment_context(db, tbl, new EnvironmentContext())

        then:
        notThrown(Exception)
        result == cols

        where:
        db = 'db1'
        tbl = 't1'
    }

    def 'test get_function'() {
        when:
        ms.get_function(null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_functions'() {
        when:
        ms.get_functions(null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_index_by_name'() {
        when:
        ms.get_index_by_name(null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_index_names'() {
        when:
        ms.get_index_names(null, null, 42 as Short)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_indexes'() {
        when:
        ms.get_indexes(null, null, 42 as Short)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_next_notification'() {
        when:
        ms.get_next_notification(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_open_txns'() {
        when:
        ms.get_open_txns()

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_open_txns_info'() {
        when:
        ms.get_open_txns_info()

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_part_specs_by_filter'() {
        when:
        ms.get_part_specs_by_filter(null, null, null, 42)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_partition no matches'() {
        given:
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            String partition_values_to_partition_filter(TableDto dto, List<String> values) throws MetaException {
                return 'filter'
            }
        }

        when:
        ms.get_partition(db, tbl, ['p1', 'p2', 'p3'])

        then:
        thrown(NoSuchObjectException)
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * hiveConverters.getNameFromPartVals(_,_) >> 'a=p1/b=p2/c=p3'
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, _, null, null, null, false, { it.includePartitionDetails })
        0 * hiveConverters.metacatToHivePartition(_, _)

        where:
        db = 'db1'
        tbl = 't1'
    }

    def 'test get_partition too many matches'() {
        given:
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            String partition_values_to_partition_filter(TableDto dto, List<String> values) throws MetaException {
                return 'filter'
            }
        }

        when:
        ms.get_partition(db, tbl, ['p1', 'p2', 'p3'])

        then:
        thrown(NoSuchObjectException)
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * hiveConverters.getNameFromPartVals(_,_) >> 'a=p1/b=p2/c=p3'
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, _, null, null, null, false, { it.includePartitionDetails }) >> partitions
        0 * hiveConverters.metacatToHivePartition(_, _)

        where:
        db = 'db1'
        tbl = 't1'
        partitions = [new PartitionDto(), new PartitionDto()]
    }

    def 'test get_partition'() {
        given:
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            String partition_values_to_partition_filter(TableDto dto, List<String> values) throws MetaException {
                return 'filter'
            }
        }

        when:
        def result = ms.get_partition(db, tbl, ['p1', 'p2', 'p3'])

        then:
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * hiveConverters.getNameFromPartVals(_,_) >> 'a=p1/b=p2/c=p3'
        1 * partitionV1.getPartitionsForRequest(_, db, tbl,_, null, null, null, false, { it.includePartitionDetails }) >> partitions
        1 * hiveConverters.metacatToHivePartition(_, _) >> matches
        result == match

        where:
        db = 'db1'
        tbl = 't1'
        partitions = [new PartitionDto()]
        match = new Partition()
        matches = [match]
    }

    def 'test get_partition_by_name no matches'() {
        when:
        ms.get_partition_by_name(db, tbl, partitionName)

        then:
        thrown(NoSuchObjectException)
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, null, null, null, null, false, { it.includePartitionDetails })
        0 * hiveConverters.metacatToHivePartition(_, _)

        where:
        db = 'db1'
        tbl = 't1'
        partitionName = 'pName'
    }

    def 'test get_partition_by_name'() {
        when:
        def result = ms.get_partition_by_name(db, tbl, partitionName)

        then:
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, null, null, null, null, false, { it.includePartitionDetails }) >> matches
        1 * hiveConverters.metacatToHivePartition(_, _) >> convertedMatches
        result == converted

        where:
        db = 'db1'
        tbl = 't1'
        partitionName = 'pName'
        matches = [new PartitionDto()]
        converted = new Partition()
        convertedMatches = [converted]
    }

    def 'test get_partition_column_statistics'() {
        when:
        ms.get_partition_column_statistics(null, null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_partition_names'() {
        when:
        def results = ms.get_partition_names(db, tbl, 42 as Short)

        then:
        notThrown(Exception)
        1 * partitionV1.getPartitionKeys(_, db, tbl, null, null, null, null, 42) >> partitions
        results == partitions

        where:
        db = 'db1'
        tbl = 't1'
        partitions = ['p1', 'p2', 'p3']
    }

    def 'test get_partition_names_ps'() {
        given:
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            String partition_values_to_partition_filter(String dbname, String tbl_name, List<String> partitionValues)
                    throws MetaException {
                return 'filter'
            }
        }

        when:
        def result = ms.get_partition_names_ps(db, tbl, ['1', '', 'three'], limit as Short)

        then:
        notThrown(Exception)
        1 * partitionV1.getPartitionKeys(_, db, tbl, 'filter', null, null, null, { it >= 0 || it == null }) >> partitions
        result == partitions

        where:
        db = 'db1'
        tbl = 't1'
        partitions = ['p1', 'p2']
        limit << [-1, 0, 1, Short.MAX_VALUE]
    }

    def 'test get_partition_with_auth'() {
        when:
        ms.get_partition_with_auth(db, tbl, partVals, null, null)

        then:
        thrown(NoSuchObjectException)
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * hiveConverters.getNameFromPartVals(_,_) >> 'a=pName'
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, null, null, null, null, false, { it.includePartitionDetails })
        0 * hiveConverters.metacatToHivePartition(_, _)

        where:
        db = 'db1'
        tbl = 't1'
        partVals = ['pName']
    }

    def 'test get_partitions'() {
        when:
        def result = ms.get_partitions(db, tbl, limit as Short)

        then:
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * partitionV1.getPartitions(_, db, tbl, null, null, null, null, { it > 0 || it == null}, false) >> partitions
        1 * hiveConverters.metacatToHivePartition(_, _) >> matches
        result == matches

        where:
        db = 'db1'
        tbl = 't1'
        partitions = [new PartitionDto()]
        matches = [new Partition()]
        limit << [-1, 0, 1, Short.MAX_VALUE]
    }

    def 'test get_partitions_by_expr'() {
        when:
        ms.get_partitions_by_expr(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_partitions_by_filter'() {
        when:
        ms.get_partition_with_auth(db, tbl, partVals, null, null)

        then:
        thrown(NoSuchObjectException)
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * hiveConverters.getNameFromPartVals(_,_) >> 'a=pName'
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, null, null, null, null, false, { it.includePartitionDetails })
        0 * hiveConverters.metacatToHivePartition(_, _)

        where:
        db = 'db1'
        tbl = 't1'
        partVals = ['pName']
    }

    def 'test get_partitions_by_names'() {
        when:
        ms.get_partitions_by_names(db, tbl, partitionNames)

        then:
        1 * metacatV1.getTable(_, db, tbl, true, false, false)
        1 * partitionV1.getPartitionsForRequest(_, db, tbl, null, null, null, null, false, { it.includePartitionDetails }) >>
                (0..2).collect { new PartitionDto() }
        3 * hiveConverters.metacatToHivePartition(_, _)

        where:
        db = 'db1'
        tbl = 't1'
        partitionNames = ['p1', 'p2', 'p3']
    }

    def 'test get_partitions_ps'() {
        given:
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            String partition_values_to_partition_filter(TableDto dto, List<String> values) throws MetaException {
                return 'filter'
            }
        }

        when:
        def result = ms.get_partitions_ps(db, tbl, ['p1', 'p2', 'p3'], limit as Short)

        then:
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto()
        1 * partitionV1.getPartitions(_, db, tbl, _, null, null, null, { it > 0 || it==null}, false) >> partitions
        1 * hiveConverters.metacatToHivePartition(_, _) >> matches
        result == matches

        where:
        db = 'db1'
        tbl = 't1'
        partitions = [new PartitionDto()]
        matches = [new Partition()]
        limit << [-1, 0, 1, Short.MAX_VALUE]
    }

    def 'test get_partitions_ps_with_auth'() {
        given:
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            String partition_values_to_partition_filter(TableDto dto, List<String> values) throws MetaException {
                return 'filter'
            }
        }

        when:
        def result = ms.get_partitions_ps_with_auth(db, tbl, ['p1', 'p2', 'p3'], limit as Short, null, null)

        then:
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto()
        1 * partitionV1.getPartitions(_, db, tbl, _, null, null, null, { it > 0 || it ==null}, false) >> partitions
        1 * hiveConverters.metacatToHivePartition(_, _) >> matches
        result == matches

        where:
        db = 'db1'
        tbl = 't1'
        partitions = [new PartitionDto()]
        matches = [new Partition()]
        limit << [-1, 0, 1, Short.MAX_VALUE]
    }

    def 'test get_partitions_pspec'() {
        when:
        ms.get_partitions_pspec(null, null, 42 as Short)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_partitions_statistics_req'() {
        when:
        ms.get_partitions_statistics_req(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_partitions_with_auth'() {
        given:
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            String partition_values_to_partition_filter(TableDto dto, List<String> values) throws MetaException {
                return 'filter'
            }
        }

        when:
        def result = ms.get_partitions_with_auth(db, tbl, limit as Short, null, null)

        then:
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto()
        1 * partitionV1.getPartitions(_, db, tbl, null, null, null, null, { it > 0 || it == null}, false) >> partitions
        1 * hiveConverters.metacatToHivePartition(_, _) >> matches
        result == matches

        where:
        db = 'db1'
        tbl = 't1'
        partitions = [new PartitionDto()]
        matches = [new Partition()]
        limit << [-1, 0, 1, Short.MAX_VALUE]
    }

    def 'test get_principals_in_role'() {
        when:
        ms.get_principals_in_role(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_privilege_set'() {
        when:
        def result = ms.get_privilege_set(null, null, null)

        then:
        result == new PrincipalPrivilegeSet(null
                , null
                , Maps.newHashMap(ImmutableMap.of("users",
                Lists.newArrayList(new PrivilegeGrantInfo("ALL", 0, "hadoop", PrincipalType.ROLE, true)))))
    }

    def 'test get_role_grants_for_principal'() {
        when:
        ms.get_role_grants_for_principal(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_role_names'() {
        when:
        ms.get_role_names()

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_schema'() {
        given:
        def cols = [new FieldSchema(name: 'c1'), new FieldSchema(name: 'c2')]
        def partitionKeys = [new FieldSchema(name: 'pk1'), new FieldSchema(name: 'pk2')]
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            Table get_table(String _dbname, String _tbl_name) throws MetaException {
                return new Table(
                        partitionKeys: partitionKeys,
                        sd: new StorageDescriptor(
                                cols: cols
                        )
                )
            }
        }

        when:
        def result = ms.get_schema(db, tbl)

        then:
        notThrown(Exception)
        result == cols + partitionKeys

        where:
        db = 'db1'
        tbl = 't1'
    }

    def 'test get_schema_with_environment_context'() {
        given:
        def cols = [new FieldSchema(name: 'c1'), new FieldSchema(name: 'c2')]
        def partitionKeys = [new FieldSchema(name: 'pk1'), new FieldSchema(name: 'pk2')]
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            Table get_table(String _dbname, String _tbl_name) throws MetaException {
                return new Table(
                        partitionKeys: partitionKeys,
                        sd: new StorageDescriptor(
                                cols: cols
                        )
                )
            }
        }

        when:
        def result = ms.get_schema_with_environment_context(db, tbl, new EnvironmentContext())

        then:
        notThrown(Exception)
        result == cols + partitionKeys

        where:
        db = 'db1'
        tbl = 't1'
    }

    def 'test get_table'() {
        when:
        def result = ms.get_table(db, tbl)

        then:
        result
        notThrown(Exception)
        1 * metacatV1.getTable(_, db, tbl, true, true, true)
        1 * hiveConverters.metacatToHiveTable(_) >> new Table()

        where:
        db = 'db1'
        tbl = 't1'
    }

    def 'test get_table_column_statistics'() {
        when:
        ms.get_table_column_statistics(null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_table_names_by_filter'() {
        when:
        def results = ms.get_table_names_by_filter(db, filter, limit as Short)

        then:
        notThrown(Exception)
        1 * metacatV1.getDatabase(_, db, false) >> { new DatabaseDto(tables: ['t1', 't2', 't3']) }
        !results.empty
        if (limit <= 0) {
            assert results.size() == 3
        } else {
            assert results.size() <= limit
        }

        where:
        db = 'db1'
        filter = 'hive_filter_field_params__presto_view = "true"'
        limit << [-1, 0, 1, Short.MAX_VALUE]
    }

    def 'test get_table_objects_by_name'() {
        when:
        def result = ms.get_table_objects_by_name(db, tableList)

        then:
        result
        result.size() == tableList.size()
        notThrown(Exception)
        1 * metacatV1.getTable(_, db, 't1', true, true, true)
        1 * metacatV1.getTable(_, db, 't2', true, true, true)
        1 * metacatV1.getTable(_, db, 't3', true, true, true)
        3 * hiveConverters.metacatToHiveTable(_) >> new Table()

        where:
        db = 'db1'
        tableList = ['t1', 't2', 't3']
    }

    def 'test get_table_statistics_req'() {
        when:
        ms.get_table_statistics_req(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_tables'() {
        given:
        metacatV1.getDatabase(_, _, false) >> new DatabaseDto(
                tables: ['atbl', 'btbl', 'ctbl', 'dtbl', 'dtbl1', 'dtbl2', 'dtbl3', 'etbl', 'ftbl']
        )

        when:
        def result = ms.get_tables('tbl', pattern)

        then:
        notThrown(Exception)
        result == expectedResult

        // I am guessing on these patterns as I have not been able to find documentation on the format
        where:
        pattern    | expectedResult
        '.*'       | ['atbl', 'btbl', 'ctbl', 'dtbl', 'dtbl1', 'dtbl2', 'dtbl3', 'etbl', 'ftbl']
        'd.*'      | ['dtbl', 'dtbl1', 'dtbl2', 'dtbl3']
        'd.*|ATBL' | ['atbl', 'dtbl', 'dtbl1', 'dtbl2', 'dtbl3']
    }

    def 'test get_type'() {
        when:
        ms.get_type(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test get_type_all'() {
        when:
        ms.get_type_all(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test grant_privileges'() {
        when:
        ms.grant_privileges(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test grant_revoke_privileges'() {
        when:
        ms.grant_revoke_privileges(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test grant_revoke_role'() {
        when:
        ms.grant_revoke_role(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test grant_role'() {
        when:
        ms.grant_role(null, null, null, null, null, false)

        then:
        thrown(InvalidOperationException)
    }

    def 'test heartbeat'() {
        when:
        ms.heartbeat(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test heartbeat_txn_range'() {
        when:
        ms.heartbeat_txn_range(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test isPartitionMarkedForEvent'() {
        when:
        ms.isPartitionMarkedForEvent(null, null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test list_privileges'() {
        when:
        ms.list_privileges(null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test list_roles'() {
        when:
        ms.list_roles(null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test lock'() {
        when:
        ms.lock(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test markPartitionForEvent'() {
        when:
        ms.markPartitionForEvent(null, null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test open_txns'() {
        when:
        ms.open_txns(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test partition_name_has_valid_characters (throws exception = false)'() {
        when:
        def result = ms.partition_name_has_valid_characters(partVals, false)

        then:
        notThrown(Exception)
        result == expectedResult

        where:
        partVals                         | expectedResult
        ['string', '1.2.3', '123', '..'] | true
    }

    def 'test partition_name_has_valid_characters (throws exception = true)'() {
        when:
        ms.partition_name_has_valid_characters(partVals, true)

        then:
        notThrown(MetaException)

        where:
        partVals = ['string', '1.2.3', '123', '..']
    }

    def 'test partition_name_to_spec'() {
        when:
        def result = ms.partition_name_to_spec(name)

        then:
        notThrown(Exception)
        result == vals

        where:
        name                                            | vals
        'dateint=20160222'                              | [dateint: '20160222']
        'dateint=20160222/region=us-west-2'             | [dateint: '20160222', region: 'us-west-2']
        'parsed=True/dateint=20160222/region=us-west-2' | [parsed: 'True', dateint: '20160222', region: 'us-west-2']
    }

    def 'test partition_name_to_vals'() {
        when:
        def result = ms.partition_name_to_vals(name)

        then:
        notThrown(Exception)
        result == vals

        where:
        name                                            | vals
        'dateint=20160222'                              | ['20160222']
        'dateint=20160222/region=us-west-2'             | ['20160222', 'us-west-2']
        'parsed=True/dateint=20160222/region=us-west-2' | ['True', '20160222', 'us-west-2']
    }

    def 'test partition_values_to_partition_filter all strings'() {
        given:
        ms = new CatalogThriftHiveMetastore(
                config, typeConverterProvider, hiveConverters, metacatV1, partitionV1, catalogName) {
            @Override
            String partition_values_to_partition_filter(TableDto dto, List<String> partitionValues)
                    throws MetaException {
                return 'filter'
            }
        }

        when:
        def result = ms.partition_values_to_partition_filter(db, tbl, ['1', '2'])

        then:
        1 * metacatV1.getTable(_, db, tbl, true, false, false)
        result == 'filter'

        where:
        db = 'db1'
        tbl = 't1'
    }

    def 'test partition_values_to_partition_filter throws an exception if too many partition_values are given'() {
        when:
        ms.partition_values_to_partition_filter(table, ['1', '2'])

        then:
        thrown(MetaException)

        where:
        db = 'db1'
        tbl = 't1'
        table = new TableDto(
                fields: (0..10).collect { new FieldDto(name: "field_$it", partition_key: false) }
        )
    }

    def 'test partition_values_to_partition_filter'() {
        when:
        def result = ms.partition_values_to_partition_filter(table, ['1', '', 'three'])

        then:
        result == "(field_0=1) OR (field_2='three')"

        where:
        db = 'db1'
        tbl = 't1'
        table = new TableDto(
                fields: (0..10).collect { new FieldDto(name: "field_$it", partition_key: it < 5) }
        )
    }

    def 'test rename_partition'() {
        when:
        ms.rename_partition(db, tbl, partVals, partition)

        then:
        1 * metacatV1.getTable(_, db, tbl, true, false, false) >> new TableDto(name: QualifiedName.ofTable(catalogName, db, tbl))
        1 * hiveConverters.getNameFromPartVals(_,_) >> 'a=pName'

        where:
        db = 'db1'
        tbl = 't1'
        partVals = ['pName']
        partition = new Partition(dbName: 'db1', tableName: 't1')
    }

    def 'test renew_delegation_token'() {
        when:
        ms.renew_delegation_token(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test revoke_privileges'() {
        when:
        ms.revoke_privileges(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test revoke_role'() {
        when:
        ms.revoke_role(null, null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test setMetaConf'() {
        when:
        ms.setMetaConf(null, null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test set_aggr_stats_for'() {
        when:
        ms.set_aggr_stats_for(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test set_ugi'() {
        when:
        def result = ms.set_ugi(user, groups)

        then:
        notThrown(Exception)
        result == ['g1', 'g2', 'g3', 'user']

        where:
        user = 'user'
        groups = ['g1', 'g2', 'g3']
    }

    def 'test show_compact'() {
        when:
        ms.show_compact(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test show_locks'() {
        when:
        ms.show_locks(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test unlock'() {
        when:
        ms.unlock(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test update_partition_column_statistics'() {
        when:
        ms.update_partition_column_statistics(null)

        then:
        thrown(InvalidOperationException)
    }

    def 'test update_table_column_statistics'() {
        when:
        ms.update_table_column_statistics(null)

        then:
        thrown(InvalidOperationException)
    }
}
