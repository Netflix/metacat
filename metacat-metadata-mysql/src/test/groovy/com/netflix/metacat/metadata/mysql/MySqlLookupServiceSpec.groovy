/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.metadata.mysql

import com.google.common.collect.Lists
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.model.Lookup
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.testdata.provider.DataDtoProvider
import org.springframework.jdbc.core.JdbcTemplate
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant

/**
 * Tests for MysqlUserMetadataService.
 * TODO: Need to move this to integration-test
 * @author amajumdar
 */

class MySqlLookupServiceSpec extends Specification {
    MySqlLookupService mySqlLookupService

    Config config
    JdbcTemplate jdbcTemplate

    def setup() {
        config = Mock(Config)
        jdbcTemplate = Mock(JdbcTemplate)

        mySqlLookupService = new MySqlLookupService(config, jdbcTemplate)
    }

    @Unroll
    def "test includeValues"() {
        setup:
        def lookupWithValues = new Lookup(
            id: 1L,
            name: 'Test1',
            values: new HashSet<String>(['tag1', 'tag2']),
            dateCreated: new Date(),
            lastUpdated: new Date()
        )
        def lookupWithoutValues = new Lookup(
            id: 2L,
            name: 'Test2',
            values: new HashSet<String>([]),
            dateCreated: new Date(),
            lastUpdated: new Date()
        )
        when:
        jdbcTemplate.queryForObject(_, _, _, _) >> (includeValues ? lookupWithValues : lookupWithoutValues)
        def lookUpResult = mySqlLookupService.addValues("tag", tagsToAdd, includeValues)

        then:
        lookUpResult.values == expectedValues
        where:
        tagsToAdd                       | includeValues | expectedValues
        ['tag1', 'tag2'] as Set<String> |true           | ['tag1', 'tag2'] as Set<String>
        ['tag1', 'tag2'] as Set<String> |false          | [] as Set<String>
        ['tag1', 'tag3'] as Set<String> |true           | ['tag1', 'tag2', 'tag3'] as Set<String>
        ['tag1', 'tag3'] as Set<String> |false          | [] as Set<String>
        ['tag3', 'tag4'] as Set<String> |true           | ['tag1', 'tag2', 'tag3', 'tag4'] as Set<String>
        ['tag3', 'tag4'] as Set<String> |false          | [] as Set<String>
    }
}
