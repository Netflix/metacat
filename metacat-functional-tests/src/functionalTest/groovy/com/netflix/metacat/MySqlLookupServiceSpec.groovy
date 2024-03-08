/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.metacat

import com.netflix.metacat.common.server.model.Lookup
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.metacat.metadata.mysql.MySqlLookupService
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import spock.lang.Shared
import spock.lang.Specification

class MySqlLookupServiceSpec extends Specification{
    private MySqlLookupService mySqlLookupService;
    private JdbcTemplate jdbcTemplate;

    @Shared
    MySqlLookupService mySqlLookupService

    @Shared
    JdbcTemplate jdbcTemplate

    boolean areLookupsEqual(Lookup l1, Lookup l2) {
        l1.id == l2.id &&
            l1.name == l2.name &&
            l1.type == l2.type &&
            l1.values == l2.values &&
            l1.dateCreated == l2.dateCreated &&
            l1.lastUpdated == l2.lastUpdated &&
            l1.createdBy == l2.createdBy &&
            l1.lastUpdatedBy == l2.lastUpdatedBy
    }

    def setupSpec() {
        String jdbcUrl = "jdbc:mysql://localhost:3306/metacat"
        String username = "metacat_user"
        String password = "metacat_user_password"

        DriverManagerDataSource dataSource = new DriverManagerDataSource()
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver")
        dataSource.setUrl(jdbcUrl)
        dataSource.setUsername(username)
        dataSource.setPassword(password)

        jdbcTemplate = new JdbcTemplate(dataSource)
        mySqlLookupService = new MySqlLookupService(new DefaultConfigImpl(new MetacatProperties()), jdbcTemplate)
    }

    def "test setValues with getValue/getValues iterative"() {
        setup:
        def values = valuesList as Set<String>
        def lookup = mySqlLookupService.setValues("mock", values)

        expect:
        lookup.values.size() == expectedSize
        lookup.values == mySqlLookupService.getValues("mock")
        lookup.values == mySqlLookupService.getValues(lookup.id)
        lookup.values.contains(mySqlLookupService.getValue("mock"))
        areLookupsEqual(lookup, mySqlLookupService.get("mock"))

        where:
        valuesList                       | expectedSize
        ["1", "2", "3"]                  | 3
        ["1", "2", "3", "4"]             | 4
        ["1", "2", "3", "3", "4"]        | 4
        ["3", "4"]                       | 2
        ["6"]                            | 1
        ["1", "6"]                       | 2
    }

    def "test setValues for different id"(){
        when:
        def mock1LookUp = mySqlLookupService.setValues("mock1", ["1", "2", "3"] as Set<String>)
        def mock2LookUp = mySqlLookupService.setValues("mock2", ["4", "5", "6"] as Set<String>)
        then:
        mock1LookUp.values == ["1", "2", "3"] as Set<String>
        mock1LookUp.values == mySqlLookupService.getValues("mock1")
        areLookupsEqual(mock1LookUp, mySqlLookupService.get("mock1"))
        mock2LookUp.values == ["4", "5", "6"] as Set<String>
        mock2LookUp.values == mySqlLookupService.getValues("mock2")
        areLookupsEqual(mock2LookUp, mySqlLookupService.get("mock2"))
    }

    def "test addValues iterative"() {
        setup:
        def values = valuesList as Set<String>
        def lookup = mySqlLookupService.addValues("mockAdd", values)

        expect:
        lookup.values.size() == expectedSize
        lookup.values == mySqlLookupService.getValues("mockAdd")
        lookup.values == mySqlLookupService.getValues(lookup.id)
        lookup.values.contains(mySqlLookupService.getValue("mockAdd"))
        areLookupsEqual(lookup, mySqlLookupService.get("mockAdd"))

        where:
        valuesList                       | expectedSize
        ["1", "2", "3"]                  | 3
        ["1", "2", "3", "4"]             | 4
        ["1", "2", "3", "3", "4"]        | 4
        ["3", "4"]                       | 4
        ["6"]                            | 5
        ["1", "6"]                       | 6
    }

    def "test addValues for different id"() {
        setup:
        def mock1LookUp = mySqlLookupService.addValues("addValues_mock1", ["1", "2", "3"] as Set<String>)
        def mock2LookUp = mySqlLookupService.addValues("addValues_mock2", ["4", "5", "6"] as Set<String>)

        expect:
        mock1LookUp.values == ["1", "2", "3"] as Set<String>
        mock1LookUp.values == mySqlLookupService.getValues("addValues_mock1")
        areLookupsEqual(mock1LookUp, mySqlLookupService.get("addValues_mock1"))
        mock2LookUp.values == ["4", "5", "6"] as Set<String>
        mock2LookUp.values == mySqlLookupService.getValues("addValues_mock2")
        areLookupsEqual(mock2LookUp, mySqlLookupService.get("addValues_mock2"))
    }
}
