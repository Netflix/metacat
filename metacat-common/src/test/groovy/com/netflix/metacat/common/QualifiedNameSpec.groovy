/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.common

import com.fasterxml.jackson.databind.node.ObjectNode
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll


class QualifiedNameSpec extends Specification {
    @Shared
    MetacatJson metacatJson = MetacatJsonLocator.INSTANCE

    def 'expect exceptions if missing required params at construction time'() {
        when:
        new QualifiedName(catalogName, databaseName, tableName, partitionName, null)

        then:
        thrown(exception)

        where:
        catalogName | databaseName | tableName | partitionName | exception
        null        | null         | null      | null          | NullPointerException
        ''          | null         | null      | null          | IllegalStateException
        '  '        | null         | null      | null          | IllegalStateException
        'c'         | null         | 't'       | null          | IllegalStateException
        'c'         | null         | null      | 'p'           | IllegalStateException
        'c'         | null         | 't'       | 'p'           | IllegalStateException
        'c'         | 'd'          | null      | 'p'           | IllegalStateException
    }

    @Unroll
    def "expect an exception trying to parse '#input'"() {
        when:
        QualifiedName.fromString(input)

        then:
        Exception e = thrown()
        e instanceof IllegalArgumentException || e instanceof IllegalStateException

        where:
        input       | _
        ''          | _
        '   '       | _
        ' /d  '     | _
        ' /d/t  '   | _
        ' /d/t/p  ' | _
    }

    @Unroll
    def 'expect to be able to convert #input into #name'() {
        expect:
        QualifiedName.fromString(input) == name

        where:
        input                           | name
        'c'                             | QualifiedName.ofCatalog('c')
        'c/'                            | QualifiedName.ofCatalog('c')
        'c/d'                           | QualifiedName.ofDatabase('c', 'd')
        ' c / d '                       | QualifiedName.ofDatabase('c', 'd')
        'c/ d '                         | QualifiedName.ofDatabase('c', 'd')
        'c/ d'                          | QualifiedName.ofDatabase('c', 'd')
        'c/d/t'                         | QualifiedName.ofTable('c', 'd', 't')
        ' c / d / t '                   | QualifiedName.ofTable('c', 'd', 't')
        'c/d/t/p=1'                     | QualifiedName.ofPartition('c', 'd', 't', 'p=1')
        ' c / d / t / p=1 '             | QualifiedName.ofPartition('c', 'd', 't', 'p=1')
        'c/d/t/p=1/trailing=1'          | QualifiedName.ofPartition('c', 'd', 't', 'p=1/trailing=1')
        ' c / d / t / p=1 / trailing=1' | QualifiedName.ofPartition('c', 'd', 't', 'p=1 / trailing=1')
    }

    @Unroll
    def 'expect to be able to convert #name into #json'() {
        expect:
        QualifiedName qualifiedName = QualifiedName.fromString(name)
        ObjectNode node = metacatJson.parseJsonObject(json)
        ObjectNode jsonQualifiedName = metacatJson.toJsonObject(qualifiedName)
        node == jsonQualifiedName

        where:
        name        | json
        'c'         | """{"qualifiedName": "c", "catalogName": "c"}"""
        'c/d'       | """{"qualifiedName": "c/d", "catalogName": "c", "databaseName": "d"}"""
        'c/d/t'     | """{"qualifiedName": "c/d/t", "catalogName": "c", "databaseName": "d", "tableName": "t"}"""
        'c/d/t/p=1' | """{"qualifiedName": "c/d/t/p=1", "catalogName": "c", "databaseName": "d", "tableName": "t", "partitionName": "p=1"}"""
    }

    def 'expect exceptions when requesting parameters that a catalog does not have'() {
        given:
        def name = QualifiedName.ofCatalog('c')

        expect:
        name.isCatalogDefinition()
        !name.isDatabaseDefinition()
        !name.isTableDefinition()
        !name.isPartitionDefinition()

        when:
        name.catalogName

        then:
        notThrown()

        when:
        name.databaseName

        then:
        thrown(IllegalStateException)

        when:
        name.tableName

        then:
        thrown(IllegalStateException)

        when:
        name.partitionName

        then:
        thrown(IllegalStateException)
    }

    def 'expect exceptions when requesting parameters that a database does not have'() {
        given:
        def name = QualifiedName.ofDatabase('c', 'd')

        expect:
        name.isCatalogDefinition()
        name.isDatabaseDefinition()
        !name.isTableDefinition()
        !name.isPartitionDefinition()

        when:
        name.catalogName

        then:
        notThrown()

        when:
        name.databaseName

        then:
        notThrown()

        when:
        name.tableName

        then:
        thrown(IllegalStateException)

        when:
        name.partitionName

        then:
        thrown(IllegalStateException)
    }

    def 'expect exceptions when requesting parameters that a table does not have'() {
        given:
        def name = QualifiedName.ofTable('c', 'd', 't')

        expect:
        name.isCatalogDefinition()
        name.isDatabaseDefinition()
        name.isTableDefinition()
        !name.isPartitionDefinition()

        when:
        name.catalogName

        then:
        notThrown()

        when:
        name.databaseName

        then:
        notThrown()

        when:
        name.tableName

        then:
        notThrown()

        when:
        name.partitionName

        then:
        thrown(IllegalStateException)
    }

    def 'expect exceptions when requesting parameters that a partition does not have'() {
        given:
        def name = QualifiedName.ofPartition('c', 'd', 't', 'p')

        expect:
        name.isCatalogDefinition()
        name.isDatabaseDefinition()
        name.isTableDefinition()
        name.isPartitionDefinition()

        when:
        name.catalogName

        then:
        notThrown()

        when:
        name.databaseName

        then:
        notThrown()

        when:
        name.tableName

        then:
        notThrown()

        when:
        name.partitionName

        then:
        notThrown()
    }
}
