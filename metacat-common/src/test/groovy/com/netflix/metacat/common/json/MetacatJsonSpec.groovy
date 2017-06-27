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

package com.netflix.metacat.common.json

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.netflix.metacat.common.dto.DtoVerificationSpec
import spock.lang.Specification
import spock.lang.Unroll

class MetacatJsonSpec extends Specification {
    MetacatJson metacatJson = new MetacatJsonLocator()

    @Unroll
    def 'can convert #clazz to json and back'() {
        given:
        def randomInstance = DtoVerificationSpec.getRandomDtoInstance(clazz)

        expect:
        def jsonString = metacatJson.toJsonString(randomInstance)
        assert jsonString
        metacatJson.parseJsonValue(jsonString, clazz).toString() == randomInstance.toString()

        where:
        clazz << DtoVerificationSpec.dtoClasses
    }

    @Unroll
    def 'dataExternal should be included with #clazz which implements HasDataMetadata'() {
        given:
        def randomInstance = DtoVerificationSpec.getRandomDtoInstance(clazz)

        expect:
        def objectNode = metacatJson.toJsonObject(randomInstance)
        objectNode.path('dataExternal').isBoolean()

        where:
        clazz << DtoVerificationSpec.hasDataMetadataClasses
    }

    @Unroll
    def 'name should be included with #clazz which has a qualified name instance'() {
        given:
        def randomInstance = DtoVerificationSpec.getRandomDtoInstance(clazz)

        expect:
        def objectNode = metacatJson.toJsonObject(randomInstance)
        objectNode.path('name').path('qualifiedName').isTextual()

        where:
        clazz << DtoVerificationSpec.hasQualifiedNameClasses
    }

    @Unroll
    def 'when merging #secondaryString into #primaryString expect #expectedString'() {
        when:
        ObjectNode primary = metacatJson.parseJsonObject(primaryString)
        ObjectNode secondary = metacatJson.parseJsonObject(secondaryString)
        ObjectNode expected = metacatJson.parseJsonObject(expectedString)
        metacatJson.mergeIntoPrimary(primary, secondary)

        then:
        primary == expected

        where:
        primaryString                                | secondaryString                                                          | expectedString
        """{}"""                                     | """{"new_field": "new_value"}"""                                         | """{"new_field": "new_value"}"""
        """{"field": "old_value"}"""                 | """{"field": "new_value"}"""                                             | """{"field": "new_value"}"""
        """{"field1": "value1"}"""                   | """{"field2": "value2"}"""                                               | """{"field1": "value1", "field2": "value2"}"""
        """{"field": "old_value"}"""                 | """{"field": {"scalar_to_object": true}}"""                              | """{"field": {"scalar_to_object": true}}"""
        """{"field": [1,2,3]}"""                     | """{"field": [4, 5, 6]}"""                                               | """{"field": [4, 5, 6]}"""
        """{"field": [1,2,3]}"""                     | """{"field": {"array_to_object": true}}"""                               | """{"field": {"array_to_object": true}}"""
        """{"field": {"overwrite_nested": false}}""" | """{"field": {"overwrite_nested": true}}"""                              | """{"field": {"overwrite_nested": true}}"""
        """{"field": {"old_nested_field": 1}}"""     | """{"field": {"new_nested_field": 2}}"""                                 | """{"field": {"old_nested_field": 1, "new_nested_field": 2}}"""
        """{"field": {"old_nested_field": 1}}"""     | """{"field": {"new_nested_field": {"new_nested_nested_field": true}}}""" | """{"field":{"old_nested_field":1,"new_nested_field":{"new_nested_nested_field":true}}}"""
    }

    @Unroll
    def 'parse json string return null for NullNode'() {
        given:
        ObjectMapper objectMapper = Mock(ObjectMapper)
        MetacatJsonLocator metacatJson = new MetacatJsonLocator(objectMapper, objectMapper)

        when:
        ObjectNode result = metacatJson.parseJsonObject("""{"field": "old_value"}""")

        then:
        objectMapper.readTree(_) >> NullNode.instance
        result == null
    }
}
