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

import com.google.common.collect.Maps
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.metacat.connector.hive.util.HiveConnectorFastServiceMetric
import com.netflix.spectator.api.NoopRegistry
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.rowset.SqlRowSet
import spock.lang.Specification

/**
 * Unit test for DirectSqlDatabase.
 * @author amajumdar
 * @since 1.3.0
 */
class DirectSqlDatabaseSpec extends Specification {
    def config = new DefaultConfigImpl(new MetacatProperties())
    def registry = new NoopRegistry()
    def context = new ConnectorContext('test', 'test', 'hive', config, registry, Maps.newHashMap())
    def metric = new HiveConnectorFastServiceMetric(registry)
    def jdbcTemplate = Mock(JdbcTemplate)
    def service = new DirectSqlDatabase(context, jdbcTemplate, metric)
    def catalogName = 'c'
    def databaseName = 'd'
    def qualifiedName = QualifiedName.ofDatabase(catalogName, databaseName)
    def databaseInfo = DatabaseInfo.builder().name(qualifiedName).uri('s3:/a/b').metadata(['a':'b']).build()
    def databaseInfo1 = DatabaseInfo.builder().name(qualifiedName).uri('s3:/a/b').metadata(['a':'b','x':'y']).build()

    def "Test get database"() {
        given:
        def rowSet = Mock(SqlRowSet)
        def paramsRowSet = Mock(SqlRowSet)
        when:
        service.getDatabase(qualifiedName)
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlDatabase.SQL.GET_DATABASE_ID,_,_,_) >> { throw new Exception()}
        thrown(Exception)
        when:
        def db = service.getDatabase(qualifiedName)
        then:
        noExceptionThrown()
        1 * jdbcTemplate.queryForObject(DirectSqlDatabase.SQL.GET_DATABASE_ID,_,_,_) >> 1L
        1 * jdbcTemplate.queryForRowSet(DirectSqlDatabase.SQL.GET_DATABASE,_,_) >> rowSet
        1 * rowSet.getString('uri') >> 's3:/a/b'
        1 * jdbcTemplate.queryForRowSet(DirectSqlDatabase.SQL.GET_DATABASE_PARAMS,_,_) >> paramsRowSet
        1 * paramsRowSet.next() >> false
        1 * rowSet.first() >> true
        db.uri == 's3:/a/b'
    }

    def "Test update database"() {
        given:
        def rowSet = Mock(SqlRowSet)
        def paramsRowSet = Mock(SqlRowSet)
        when:
        service.update(databaseInfo)
        then:
        1 * jdbcTemplate.queryForObject(DirectSqlDatabase.SQL.GET_DATABASE_ID,_,_,_) >> { throw new EmptyResultDataAccessException(1)}
        thrown(DatabaseNotFoundException)
        when:
        service.update(databaseInfo)
        then:
        noExceptionThrown()
        1 * jdbcTemplate.queryForObject(DirectSqlDatabase.SQL.GET_DATABASE_ID,_,_,_) >> 1L
        1 * jdbcTemplate.queryForRowSet(DirectSqlDatabase.SQL.GET_DATABASE,_,_) >> rowSet
        1 * rowSet.getString('uri') >> 's3:/a/b'
        1 * jdbcTemplate.queryForRowSet(DirectSqlDatabase.SQL.GET_DATABASE_PARAMS,_,_) >> paramsRowSet
        1 * paramsRowSet.next() >> false
        1 * rowSet.first() >> true
        when:
        service.update(databaseInfo)
        then:
        noExceptionThrown()
        1 * jdbcTemplate.queryForObject(DirectSqlDatabase.SQL.GET_DATABASE_ID,_,_,_) >> 1L
        1 * jdbcTemplate.queryForRowSet(DirectSqlDatabase.SQL.GET_DATABASE,_,_) >> rowSet
        1 * rowSet.getString('uri') >> 's3:/a/b'
        1 * jdbcTemplate.queryForRowSet(DirectSqlDatabase.SQL.GET_DATABASE_PARAMS,_,_) >> paramsRowSet
        2 * paramsRowSet.next() >>> [true, false]
        1 * paramsRowSet.getString('param_key') >> 'a'
        1 * paramsRowSet.getString('param_value') >> 'c'
        0 * jdbcTemplate.batchUpdate(DirectSqlDatabase.SQL.INSERT_DATABASE_PARAMS,_,_)
        1 * jdbcTemplate.batchUpdate(DirectSqlDatabase.SQL.UPDATE_DATABASE_PARAMS,_,_)
        1 * rowSet.first() >> true
        when:
        service.update(databaseInfo1)
        then:
        noExceptionThrown()
        1 * jdbcTemplate.queryForObject(DirectSqlDatabase.SQL.GET_DATABASE_ID,_,_,_) >> 1L
        1 * jdbcTemplate.queryForRowSet(DirectSqlDatabase.SQL.GET_DATABASE,_,_) >> rowSet
        1 * rowSet.getString('uri') >> 's3:/a/b'
        1 * jdbcTemplate.queryForRowSet(DirectSqlDatabase.SQL.GET_DATABASE_PARAMS,_,_) >> paramsRowSet
        2 * paramsRowSet.next() >>> [true, false]
        1 * paramsRowSet.getString('param_key') >> 'a'
        1 * paramsRowSet.getString('param_value') >> 'c'
        1 * jdbcTemplate.batchUpdate(DirectSqlDatabase.SQL.INSERT_DATABASE_PARAMS,_,_)
        1 * jdbcTemplate.batchUpdate(DirectSqlDatabase.SQL.UPDATE_DATABASE_PARAMS,_,_)
        1 * rowSet.first() >> true
    }
}
