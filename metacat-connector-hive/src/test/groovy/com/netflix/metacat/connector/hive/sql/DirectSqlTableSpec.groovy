package com.netflix.metacat.connector.hive.sql

import com.google.common.collect.Maps
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.exception.ConnectorException
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.converter.DozerTypeConverter
import com.netflix.metacat.common.server.converter.TypeConverterFactory
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.metacat.common.server.util.ThreadServiceManager
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.connector.hive.util.HiveConnectorFastServiceMetric
import com.netflix.metacat.testdata.provider.DataDtoProvider
import com.netflix.spectator.api.NoopRegistry
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import spock.lang.Specification

import java.time.Instant
import java.util.function.Supplier

/**
 * Unit test for DirectSqlTable.
 * @author amajumdar
 * @since 1.2.0
 */
class DirectSqlTableSpec extends Specification {
    def config = new DefaultConfigImpl(new MetacatProperties())
    def registry = new NoopRegistry()
    def context = new ConnectorContext("test", config, registry, Maps.newHashMap())
    def metric = new HiveConnectorFastServiceMetric(registry)
    def jdbcTemplate = Mock(JdbcTemplate)
    def service = new DirectSqlTable(context, jdbcTemplate, metric)
    def catalogName = 'c'
    def databaseName = 'd'
    def tableName = 't'
    def qualifiedName = QualifiedName.ofTable(catalogName, databaseName, tableName)
    def tableUpdateService = Mock(Supplier)

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


    def "Test iceberg table lock and update"() {
        given:
        def tableId = 1234
        when:
        service.lockIcebergTable(tableId)
        then:
        1 * jdbcTemplate.update(DirectSqlTable.SQL.INSERT_TABLE_PARAMS,_)
        1 * jdbcTemplate.queryForObject(DirectSqlTable.SQL.ICEBERG_TABLE_LOCK,_,_)
        noExceptionThrown()
        when:
        service.lockIcebergTable(tableId)
        then:
        1 * jdbcTemplate.update(DirectSqlTable.SQL.INSERT_TABLE_PARAMS,_)
        1 * jdbcTemplate.queryForObject(DirectSqlTable.SQL.ICEBERG_TABLE_LOCK,_,_) >> {throw new EmptyResultDataAccessException(1)}
        noExceptionThrown()
        when:
        service.lockIcebergTable(tableId)
        then:
        1 * jdbcTemplate.update(DirectSqlTable.SQL.UPDATE_TABLE_PARAMS,_)
        1 * jdbcTemplate.queryForObject(DirectSqlTable.SQL.ICEBERG_TABLE_LOCK,_,_) >> false
        noExceptionThrown()
        when:
        service.lockIcebergTable(tableId)
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlTable.SQL.ICEBERG_TABLE_LOCK,_,_) >> true
        thrown(IllegalStateException)
    }

    def "Test iceberg table unlock"() {
        given:
        def tableId = 1234
        when:
        service.unlockIcebergTable(tableId)
        then:
        1 * jdbcTemplate.update(DirectSqlTable.SQL.UPDATE_TABLE_PARAMS,_)
        noExceptionThrown()
        when:
        service.unlockIcebergTable(tableId)
        then:
        1 * jdbcTemplate.update(DirectSqlTable.SQL.UPDATE_TABLE_PARAMS,_) >> {throw new Exception()}
        thrown(Exception)
    }
}
