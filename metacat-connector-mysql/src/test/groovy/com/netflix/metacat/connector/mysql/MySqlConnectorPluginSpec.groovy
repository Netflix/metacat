/*
 *
 *  Copyright 2017 Netflix, Inc.
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
 *
 */
package com.netflix.metacat.connector.mysql

import spock.lang.Specification

/**
 * Tests for the MySqlConnectorPlugin class.
 *
 * @author tgianos
 * @since 1.0.0
 */
class MySqlConnectorPluginSpec extends Specification {

    def plugin = new MySqlConnectorPlugin()

    def "Can get connector type"() {
        when:
        def type = this.plugin.getType()

        then:
        type == "mysql"
    }

    def "Can get connector type converter"() {
        when:
        def converter = this.plugin.getTypeConverter()

        then:
        converter instanceof MySqlTypeConverter
    }
}
