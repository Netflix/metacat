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

import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.metacat.metadata.mysql.MySqlLookupService
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import spock.lang.Shared
import spock.lang.Specification

class MySqlLookupServiceUnitTest extends Specification{
    private MySqlLookupService mySqlLookupService;
    private JdbcTemplate jdbcTemplate;

    @Shared
    MySqlLookupService mySqlLookupService

    @Shared
    JdbcTemplate jdbcTemplate

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
        // ... additional setup
    }

    def "test get method"() {
        when:
        def lookup = mySqlLookupService.setValues("mock", ["1", "2", "3"] as Set<String>)
        then:
        lookup.values.size() == 3
        when:
        lookup = mySqlLookupService.setValues("mock", ["1", "2", "3", "4"] as Set<String>)
        then:
        lookup.values.size() == 4
        when:
        lookup = mySqlLookupService.setValues("mock", ["1", "2", "3", "3", "4"] as Set<String>)
        then:
        lookup.values.size() == 4
        when:
        lookup = mySqlLookupService.setValues("mock", ["3", "4"] as Set<String>)
        then:
        lookup.values.size() == 2
        when:
        lookup = mySqlLookupService.setValues("mock", ["6"] as Set<String>)
        then:
        lookup.values.size() == 1
        when:
        lookup = mySqlLookupService.setValues("mock", ["1", "6"] as Set<String>)
        then:
        lookup.values.size() == 0
    }
}
