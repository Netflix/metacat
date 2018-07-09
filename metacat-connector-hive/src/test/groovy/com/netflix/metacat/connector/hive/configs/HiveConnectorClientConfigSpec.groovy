/*
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.metacat.connector.hive.configs

import com.netflix.metacat.common.server.connectors.ConnectorContext
import spock.lang.Specification

import javax.sql.DataSource

/**
 * HiveConnectorClientConfigSpec.
 * @author amajumdar
 * @since 1.2.0
 */
class HiveConnectorClientConfigSpec extends Specification{
    def dataSource = Mock(DataSource)
    def context = Mock(ConnectorContext)
    def config = new HiveConnectorClientConfig()
    def testHiveReadJdbcTemplate(){
        when:
        def template = config.hiveReadJdbcTemplate(context, dataSource)
        then:
        template.getQueryTimeout() == 120
        1 * context.getConfiguration() >> ['javax.jdo.option.DatastoreReadTimeoutMillis':'invalid']
        when:
        template = config.hiveReadJdbcTemplate(context, dataSource)
        then:
        template.getQueryTimeout() == 10
        1 * context.getConfiguration() >> ['javax.jdo.option.DatastoreReadTimeoutMillis':'10000']
        when:
        template = config.hiveReadJdbcTemplate(context, dataSource)
        then:
        template.getQueryTimeout() == 120
        1 * context.getConfiguration() >> [:]
    }
    def testGetDefaultConf(){
        when:
        def conf = config.getDefaultConf(context)
        then:
        conf.get('javax.jdo.option.DatastoreTimeout') == '60000'
        2 * context.getConfiguration() >> ['javax.jdo.option.DatastoreTimeout':'invalid']
        when:
        conf = config.getDefaultConf(context)
        then:
        conf.get('javax.jdo.option.DatastoreTimeout') == '10000'
        2 * context.getConfiguration() >> ['javax.jdo.option.DatastoreTimeout':'10000']
        when:
        conf = config.getDefaultConf(context)
        then:
        conf.get('javax.jdo.option.DatastoreTimeout') == '60000'
        2 * context.getConfiguration() >> [:]
    }
}
