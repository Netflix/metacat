package com.netflix.metacat.connector.hive.sql

import com.google.common.collect.Maps
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.exception.ConnectorException
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.converter.DefaultTypeConverter
import com.netflix.metacat.common.server.converter.DozerJsonTypeConverter
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
import org.springframework.dao.QueryTimeoutException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.StatementCallback
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLTimeoutException
import java.sql.Statement
import java.time.Instant

/**
 * Unit test for DirectSqlGetPartition.
 * @author amajumdar
 * @since 1.1.0
 */
class DirectSqlGetPartitionSpec extends Specification {
    def config = new DefaultConfigImpl(new MetacatProperties())
    def typeFactory = new TypeConverterFactory(new DefaultTypeConverter())
    def converter = new ConverterUtil(new DozerTypeConverter(typeFactory), new DozerJsonTypeConverter(typeFactory))
    def registry = new NoopRegistry()
    def context = DataDtoProvider.newContext(config, null)
    def metric = new HiveConnectorFastServiceMetric(registry)
    def jdbcTemplate = Mock(JdbcTemplate)
    def service = new DirectSqlGetPartition(context, new ThreadServiceManager(registry, config), jdbcTemplate, metric)
    def hiveConverter = new HiveConnectorInfoConverter(new HiveTypeConverter())
    def catalogName = 'c'
    def databaseName = 'd'
    def tableName = 't'
    def partitionName = 'field1=a/field3=b'
    def qualifiedName = QualifiedName.ofTable(catalogName, databaseName, tableName)
    def userName = "amajumdar"
    def tableInfo = converter.fromTableDto(DataDtoProvider.getTable(catalogName, databaseName, tableName, userName, 's3:/a/b'))
    def table = hiveConverter.fromTableInfo(tableInfo)
    def partitionInfos = DataDtoProvider.getPartitions(catalogName, databaseName, tableName, partitionName, 's3:/a/b', 5).collect { converter.fromPartitionDto(it)}
    def partitionHolders = partitionInfos.collect { new PartitionHolder(1,1,1,it)}
    def partitionNames = partitionInfos.collect { it.name.partitionName}
    def partitionuris = partitionInfos.collect { it.serde.uri}
    def partitionTableDbNames = partitionInfos.collect { [it.name.partitionName, it.name.tableName, it.name.databaseName]}
    def requestContext = new ConnectorRequestContext(timestamp: Instant.now().toEpochMilli(), userName:'test')
    def emptyPartitionsRequest = new PartitionListRequest()
    def partitionsRequest = new PartitionListRequest(filter:"field1='a'", sort: new Sort('createdDate', SortOrder.ASC), pageable: new Pageable(3,1))

    def "Test getting partition count"() {
        when:
        service.getPartitionCount(requestContext, qualifiedName)
        then:
        1 * jdbcTemplate.query(DirectSqlGetPartition.SQL.SQL_GET_PARTITION_COUNT,_,_,_) >> { throw new Exception()}
        thrown(ConnectorException)
        when:
        def count = service.getPartitionCount(requestContext, qualifiedName)
        then:
        1 * jdbcTemplate.query(DirectSqlGetPartition.SQL.SQL_GET_PARTITION_COUNT,_,_,_) >> 2
        noExceptionThrown()
        count == 2
    }

    def "Test getting partition keys"() {
        when:
        def keys = service.getPartitionKeys(requestContext, qualifiedName, emptyPartitionsRequest)
        then:
        1 * jdbcTemplate.query(_,_,_) >> partitionNames
        noExceptionThrown()
        keys.size() == partitionNames.size()
        when:
        keys = service.getPartitionKeys(requestContext, qualifiedName, partitionsRequest)
        then:
        1 * jdbcTemplate.query(_,_,_) >> partitionNames
        noExceptionThrown()
        keys.size() == 3
    }


    def "Test getting partition uris"() {
        when:
        def uris = service.getPartitionUris(requestContext, qualifiedName, emptyPartitionsRequest)
        then:
        1 * jdbcTemplate.query(_,_,_) >> partitionuris
        noExceptionThrown()
        uris.size() == partitionNames.size()
        when:
        uris = service.getPartitionUris(requestContext, qualifiedName, partitionsRequest)
        then:
        1 * jdbcTemplate.query(_,_,_) >> partitionuris
        noExceptionThrown()
        uris.size() == 3
    }

    def "Test getting partitions by names"() {
        when:
        def holders = service.getPartitionHoldersByNames(table, partitionNames, true)
        then:
        1 * jdbcTemplate.query(_,_,_) >> partitionHolders
        noExceptionThrown()
        holders.size() == partitionHolders.size()
    }

    def "Test query timeout"() {
        given:
        def source = Mock(DataSource)
        def conn = Mock(Connection)
        source.getConnection() >> conn
        conn.createStatement() >> Mock(Statement)
        def template = new JdbcTemplate(source)
        def action = Mock(StatementCallback)
        when:
        template.execute(action)
        then:
        thrown(QueryTimeoutException)
        1 * action.doInStatement(_) >> {throw new SQLTimeoutException()}
    }
}
