package com.netflix.metacat.connector.hive.sql

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.exception.ConnectorException
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.metacat.connector.hive.util.HiveConnectorFastServiceMetric
import com.netflix.metacat.testdata.provider.DataDtoProvider
import com.netflix.spectator.api.NoopRegistry
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.Warehouse
import org.springframework.dao.CannotAcquireLockException
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Connection
import java.util.function.Supplier

/**
 * Unit test for DirectSqlTable.
 * @author amajumdar
 * @since 1.2.0
 */
class DirectSqlTableSpec extends Specification {
    def config = new DefaultConfigImpl(new MetacatProperties())
    def registry = new NoopRegistry()
    def context = DataDtoProvider.newContext(config, null)
    def metric = new HiveConnectorFastServiceMetric(registry)
    def jdbcTemplate = Mock(JdbcTemplate)
    def directSqlSavePartition = Mock(DirectSqlSavePartition)
    def fs = Mock(FileSystem)
    def warehouse = Mock(Warehouse)
    def service = new DirectSqlTable(context, jdbcTemplate, metric, directSqlSavePartition, warehouse)
    def catalogName = 'c'
    def databaseName = 'd'
    def tableName = 't'
    def qualifiedName = QualifiedName.ofTable(catalogName, databaseName, tableName)
    def tableUpdateService = Mock(Supplier)
    def dataSource = Mock(DataSource)
    def connection = Mock(Connection)

    def "Test table exists check"() {
        when:
        service.exists(qualifiedName)
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlTable.SQL.EXIST_TABLE_BY_NAME,_,_,_) >> { throw new Exception()}
        thrown(Exception)
        when:
        def exists = service.exists(qualifiedName)
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlTable.SQL.EXIST_TABLE_BY_NAME,_,_,_) >> 1
        noExceptionThrown()
        exists
    }

    def "Test getting table names"() {
        when:
        def keys = service.getTableNames(['s3:/a/b'], true)
        then:
        1 * jdbcTemplate.query(_,_,_) >> ['s3:/a/b':[qualifiedName]]
        noExceptionThrown()
        keys.size() == 1
        when:
        keys = service.getTableNames(['s3:/a/b'], false)
        then:
        1 * jdbcTemplate.query(_,_,_) >> ['s3:/a/b':[qualifiedName],'s3:/a/c':[qualifiedName]]
        noExceptionThrown()
        keys.size() == 2
    }


    def "Test iceberg table update"() {
        given:
        def table = new TableInfo(name: qualifiedName, metadata: ['metadata_location':'s3:/c/d/t1', 'previous_metadata_location':'s3:/c/d/t'])
        when:
        service.updateIcebergTable(table)
        then:
        1 * jdbcTemplate.batchUpdate(DirectSqlTable.SQL.INSERT_TABLE_PARAMS,_,_)
        1 * jdbcTemplate.batchUpdate(DirectSqlTable.SQL.UPDATE_TABLE_PARAMS,_,_)
        1 * jdbcTemplate.query(DirectSqlTable.SQL.TABLE_PARAMS_LOCK,_,_) >> ['table_type':DirectSqlTable.ICEBERG_TABLE_TYPE, 'metadata_location':'s3:/c/d/t']
        1 * warehouse.getFs(_) >> fs
        1 * fs.exists(_) >> true
        noExceptionThrown()
        when:
        service.updateIcebergTable(table)
        then:
        0 * jdbcTemplate.batchUpdate(DirectSqlTable.SQL.INSERT_TABLE_PARAMS,_,_)
        1 * jdbcTemplate.batchUpdate(DirectSqlTable.SQL.UPDATE_TABLE_PARAMS,_,_)
        1 * jdbcTemplate.query(DirectSqlTable.SQL.TABLE_PARAMS_LOCK,_,_) >> ['table_type':DirectSqlTable.ICEBERG_TABLE_TYPE, 'metadata_location':'s3:/c/d/t', 'previous_metadata_location':'']
        1 * warehouse.getFs(_) >> fs
        1 * fs.exists(_) >> true
        noExceptionThrown()
        when:
        service.updateIcebergTable(table)
        then:
        0 * jdbcTemplate.batchUpdate(DirectSqlTable.SQL.INSERT_TABLE_PARAMS,_,_)
        0 * jdbcTemplate.batchUpdate(DirectSqlTable.SQL.UPDATE_TABLE_PARAMS,_,_)
        1 * jdbcTemplate.query(DirectSqlTable.SQL.TABLE_PARAMS_LOCK,_,_) >> ['table_type':DirectSqlTable.ICEBERG_TABLE_TYPE, 'metadata_location':'s3:/c/d/t1', 'previous_metadata_location':'s3:/c/d/t']
        1 * warehouse.getFs(_) >> fs
        1 * fs.exists(_) >> true
        noExceptionThrown()
        when:
        service.updateIcebergTable(new TableInfo(name: qualifiedName, metadata: ['metadata_location':'s3:/c/d/t1']))
        then:
        0 * jdbcTemplate.batchUpdate(DirectSqlTable.SQL.INSERT_TABLE_PARAMS,_,_)
        0 * jdbcTemplate.batchUpdate(DirectSqlTable.SQL.UPDATE_TABLE_PARAMS,_,_)
        1 * jdbcTemplate.query(DirectSqlTable.SQL.TABLE_PARAMS_LOCK,_,_) >> ['table_type':DirectSqlTable.ICEBERG_TABLE_TYPE, 'metadata_location':'s3:/c/d/t1']
        0 * warehouse.getFs(_) >> fs
        noExceptionThrown()
        when:
        service.updateIcebergTable(table)
        then:
        1 * jdbcTemplate.query(DirectSqlTable.SQL.TABLE_PARAMS_LOCK,_,_) >> [:]
        thrown(InvalidMetaException)
        when:
        service.updateIcebergTable(table)
        then:
        1 * jdbcTemplate.query(DirectSqlTable.SQL.TABLE_PARAMS_LOCK,_,_) >> {throw new EmptyResultDataAccessException(1)}
        thrown(InvalidMetaException)
        when:
        service.updateIcebergTable(table)
        then:
        1 * jdbcTemplate.query(DirectSqlTable.SQL.TABLE_PARAMS_LOCK,_,_) >> ['table_type':'invalid']
        thrown(InvalidMetaException)
    }

    def "Test delete table"() {
        when:
        service.delete(qualifiedName)
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlTable.SQL.TABLE_SEQUENCE_IDS,_,_,_) >> {throw new EmptyResultDataAccessException(1)}
        thrown(TableNotFoundException)
        when:
        service.delete(qualifiedName)
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlTable.SQL.TABLE_SEQUENCE_IDS,_,_,_) >> new TableSequenceIds(1,1,1,1)
        1 * directSqlSavePartition.delete(qualifiedName) >> {throw new CannotAcquireLockException('a')}
        thrown(ConnectorException)
        when:
        service.delete(qualifiedName)
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlTable.SQL.TABLE_SEQUENCE_IDS,_,_,_) >> new TableSequenceIds(1,1,1,1)
        noExceptionThrown()
    }

    def "Test get-connection"() {
        given:
        jdbcTemplate.getDataSource() >> dataSource

        and:
        dataSource.getConnection() >> connection

        when:
        def actualConnection = service.getConnection()

        then:
        actualConnection == connection
    }

    def "Test get-connection when no data source configured"() {
        when:
        service.getConnection()

        then:
        thrown(NullPointerException)
    }
}
