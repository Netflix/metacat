package com.netflix.metacat.connector.hive.sql

import com.google.common.collect.Maps
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.exception.AlreadyExistsException
import com.netflix.metacat.common.server.connectors.exception.ConnectorException
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.converter.DefaultTypeConverter
import com.netflix.metacat.common.server.converter.DozerJsonTypeConverter
import com.netflix.metacat.common.server.converter.DozerTypeConverter
import com.netflix.metacat.common.server.converter.TypeConverterFactory
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.connector.hive.util.HiveConnectorFastServiceMetric
import com.netflix.metacat.testdata.provider.DataDtoProvider
import com.netflix.spectator.api.NoopRegistry
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import spock.lang.Specification

/**
 * Unit test for DirectSqlSavePartition.
 * @author amajumdar
 * @since 1.1.0
 */
class DirectSqlSavePartitionSpec extends Specification {
    def config = new DefaultConfigImpl(new MetacatProperties(null))
    def typeFactory = new TypeConverterFactory(new DefaultTypeConverter())
    def converter = new ConverterUtil(new DozerTypeConverter(typeFactory), new DozerJsonTypeConverter(typeFactory))
    def registry = new NoopRegistry()
    def context = DataDtoProvider.newContext(config, null)
    def metric = new HiveConnectorFastServiceMetric(registry)
    def jdbcTemplate = Mock(JdbcTemplate)
    def service = new DirectSqlSavePartition(context, jdbcTemplate, new SequenceGeneration(jdbcTemplate), metric)
    def hiveConverter = new HiveConnectorInfoConverter(new HiveTypeConverter())
    def catalogName = 'c'
    def databaseName = 'd'
    def tableName = 't'
    def invalidPartitionName = 'one=xyz'
    def partitionName = 'field1=a/field3=b'
    def qualifiedName = QualifiedName.ofTable(catalogName, databaseName, tableName)
    def userName = "amajumdar"
    def tableInfo = converter.fromTableDto(DataDtoProvider.getTable(catalogName, databaseName, tableName, userName, 's3:/a/b'))
    def table = hiveConverter.fromTableInfo(tableInfo)
    def invalidPartitionInfos = DataDtoProvider.getPartitions(catalogName, databaseName, tableName, invalidPartitionName, 's3:/a/b', 5).collect { converter.fromPartitionDto(it)}
    def partitionInfos = DataDtoProvider.getPartitions(catalogName, databaseName, tableName, partitionName, 's3:/a/b', 5).collect { converter.fromPartitionDto(it)}
    def partitionHolders = partitionInfos.collect { new PartitionHolder(1,1,1,it)}
    def partitionNames = partitionInfos.collect { it.name.partitionName}

    def "Test insert of partitions"() {
        when:
        service.insert(qualifiedName, table, partitionInfos)
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlSavePartition.SQL.TABLE_SELECT, _, _) >> { throw new EmptyResultDataAccessException(1)}
        thrown(TableNotFoundException)
        when:
        service.insert(qualifiedName, table, invalidPartitionInfos)
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlSavePartition.SQL.TABLE_SELECT,_,_) >> new TableSequenceIds(1,1)
        3 * jdbcTemplate.queryForObject(SequenceGeneration.SQL.SEQUENCE_NEXT_VAL_BYNAME,_,_)
        thrown(InvalidMetaException)
        when:
        service.insert(qualifiedName, table, partitionInfos)
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlSavePartition.SQL.TABLE_SELECT,_,_) >> new TableSequenceIds(1,1)
        3 * jdbcTemplate.queryForObject(SequenceGeneration.SQL.SEQUENCE_NEXT_VAL_BYNAME,_,_)
        noExceptionThrown()
    }

    def "Test update of partitions"() {
        when:
        service.update(qualifiedName, partitionHolders)
        then:
        1 * jdbcTemplate.batchUpdate(DirectSqlSavePartition.SQL.SERDES_UPDATE,_,_) >> { throw new DuplicateKeyException('a')}
        thrown(AlreadyExistsException)
        when:
        service.update(qualifiedName, partitionHolders)
        then:
        1 * jdbcTemplate.batchUpdate(DirectSqlSavePartition.SQL.SERDES_UPDATE,_,_)
        noExceptionThrown()
    }

    def "Test delete of partitions"() {
        when:
        service.delete(qualifiedName, partitionNames)
        then:
        1 * jdbcTemplate.query(_,_,_) >> { throw new EmptyResultDataAccessException(1)}
        noExceptionThrown()
        when:
        service.delete(qualifiedName, partitionNames)
        then:
        1 * jdbcTemplate.query(_,_,_) >> { throw new Exception()}
        thrown(ConnectorException)
        when:
        service.delete(qualifiedName, partitionNames)
        then:
        1 * jdbcTemplate.query(_,_,_) >> [new PartitionSequenceIds()]
        8 * jdbcTemplate.update(_,_)
        noExceptionThrown()
    }

    def "Test add/update/delete of partitions"() {
        when:
        service.addUpdateDropPartitions(qualifiedName, table, partitionInfos, partitionHolders, new HashSet<String>(partitionNames))
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlSavePartition.SQL.TABLE_SELECT,_,_) >> new TableSequenceIds(1,1)
        3 * jdbcTemplate.queryForObject(SequenceGeneration.SQL.SEQUENCE_NEXT_VAL_BYNAME,_,_)
        1 * jdbcTemplate.query(_,_,_) >> [new PartitionSequenceIds()]
        11 * jdbcTemplate.update(_,_)
        noExceptionThrown()
    }
}
