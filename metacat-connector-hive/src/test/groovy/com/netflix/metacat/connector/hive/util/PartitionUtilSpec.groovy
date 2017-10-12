package com.netflix.metacat.connector.hive.util

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.Table
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for PartitionUtil.
 * @author amajumdar
 * @since 1.1.0
 */
@Unroll
class PartitionUtilSpec extends Specification {
    def catalogName = 'c'
    def databaseName = 'd'
    def tableName = 't'
    def qualifiedName = QualifiedName.ofTable(catalogName, databaseName, tableName)
    def table = new Table(dbName: databaseName, tableName: tableName,
        partitionKeys: [new FieldSchema('field1', 'string', ''), new FieldSchema('field2', 'int', '')])

    def "Partition values for #name"() {
        expect:
        PartitionUtil.getPartValuesFromPartName(qualifiedName, table, name) == result
        where:
        name                    | result
        'field1=a/field2=1'     | ['a','1']
        'field1=a b/field2=1'   | ['a b','1']
        'field1=a%20b/field2=1' | ['a b','1']
        'field1= a/field2=1'    | [' a','1']
        'field1=a&b/field2=11'  | ['a&b','11']
        'field1=a&b/field2=1/1' | ['a&b','1']
    }

    def "Invalid partition name #name throws exception"() {
        when:
        PartitionUtil.getPartValuesFromPartName(qualifiedName, table, name)
        then:
        thrown(InvalidMetaException)
        where:
        name << ['field1=a/field3=1',
                 'field1=a/',
                 'field2=a/field1=',
                 'field1=a/field2/']

    }

    def "Escape partition #name"() {
        expect:
        PartitionUtil.escapePartitionName(name) == result
        where:
        name                    | result
        'field1=a/field2=1'     | 'field1=a/field2=1'
        'field1=a%20b/field2=1' | 'field1=a b/field2=1'
    }

    def "Make partition name #result from values"() {
        expect:
        PartitionUtil.makePartName(table.getPartitionKeys(), values) == result
        where:
        values          | result
        ['a','1']       | 'field1=a/field2=1'
        ['a b','1']     | 'field1=a b/field2=1'
        ['a b','1']     | 'field1=a b/field2=1'
        ['a#b','1']     | 'field1=a%23b/field2=1'
        [' a','1']      | 'field1= a/field2=1'
        ['a&b','11']    | 'field1=a&b/field2=11'
        ['a&b','1']     | 'field1=a&b/field2=1'
    }
}
