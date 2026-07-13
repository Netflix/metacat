/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.common.dto.notifications.sns.payloads

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.fge.jsonpatch.diff.JsonDiff
import com.google.common.collect.Lists
import com.netflix.metacat.common.dto.TableDto
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Tests for the UploadPayload class.
 *
 * @author tgianos
 * @since 0.1.47
 */
class UpdatePayloadSpec extends Specification {

    def mapper = new ObjectMapper()

    def "can construct with variables saved"() {
        def previous = Mock(TableDto)
        def current = Mock(TableDto)
        def patch = JsonDiff.asJsonPatch(this.mapper.readTree("{\"a\":\"b\"}"), this.mapper.readTree("{\"a\":\"c\"}"))

        when:
        def updatePayload = new UpdatePayload<TableDto>(previous, patch)

        then:
        updatePayload != null
        updatePayload.getPatch() == patch
        updatePayload.getPrevious() == previous
    }

    def "can serialize and deserialize with no data loss"() {
        def previous = Lists.newArrayList("one", "three")
        def current = Lists.newArrayList("one", "two", "three")
        def patch = JsonDiff.asJsonPatch(this.mapper.valueToTree(previous), this.mapper.valueToTree(current))
        UpdatePayload<ArrayList<String>> payload = new UpdatePayload<>(previous, patch)

        when: "Serialize to JSON and then back to a POJO and back to JSON"
        def json = this.mapper.writeValueAsString(payload)
        UpdatePayload<ArrayList<String>> payload2 = this.mapper.readValue(
                json,
                new TypeReference<UpdatePayload<ArrayList<String>>>() {}
        )
        def json2 = this.mapper.writeValueAsString(payload2)

        then: "Make sure all the values are still equal"
        payload.getPatch().toString() == payload2.getPatch().toString()
        payload.getPrevious() == payload2.getPrevious()
        json == json2
    }

    def "the two-arg constructor leaves metadataOnly false"() {
        def previous = Lists.newArrayList("one")
        def patch = JsonDiff.asJsonPatch(this.mapper.valueToTree(previous), this.mapper.valueToTree(previous))

        expect:
        !new UpdatePayload<>(previous, patch).isMetadataOnly()
    }

    def "metadataOnly is always serialized"() {
        def previous = Lists.newArrayList("one")
        def patch = JsonDiff.asJsonPatch(this.mapper.valueToTree(previous), this.mapper.valueToTree(previous))

        expect:
        this.mapper.writeValueAsString(new UpdatePayload<>(previous, patch, true)).contains('"metadataOnly":true')
        this.mapper.writeValueAsString(new UpdatePayload<>(previous, patch, false)).contains('"metadataOnly":false')
    }

    @Unroll
    def "round-trips metadataOnly = #value with no data loss"() {
        def previous = Lists.newArrayList("one", "three")
        def patch = JsonDiff.asJsonPatch(this.mapper.valueToTree(previous), this.mapper.valueToTree(previous))
        def payload = new UpdatePayload<>(previous, patch, value)

        when:
        def json = this.mapper.writeValueAsString(payload)
        UpdatePayload<ArrayList<String>> payload2 = this.mapper.readValue(
                json, new TypeReference<UpdatePayload<ArrayList<String>>>() {})

        then:
        payload2.isMetadataOnly() == value

        where:
        value << [true, false]
    }

    def "deserializes a legacy payload that predates metadataOnly (backward compatible)"() {
        given: "JSON with no metadataOnly field, as produced before the field existed"
        def previous = Lists.newArrayList("one")
        def patch = JsonDiff.asJsonPatch(this.mapper.valueToTree(previous), this.mapper.valueToTree(previous))
        def legacyJson = '{"previous":["one"],"patch":' + this.mapper.writeValueAsString(patch) + '}'

        when:
        UpdatePayload<ArrayList<String>> payload = this.mapper.readValue(
                legacyJson, new TypeReference<UpdatePayload<ArrayList<String>>>() {})

        then:
        !payload.isMetadataOnly()
        payload.getPrevious() == ["one"]
    }

    def "tolerates unknown future fields on a strict mapper (forward compatible)"() {
        given: "a payload JSON carrying a field this version does not know about"
        def previous = Lists.newArrayList("one")
        def patch = JsonDiff.asJsonPatch(this.mapper.valueToTree(previous), this.mapper.valueToTree(previous))
        def json = this.mapper.writeValueAsString(new UpdatePayload<>(previous, patch, true))
        def futureJson = json.substring(0, json.length() - 1) + ',"someFutureField":"x"}'

        when: "deserialized with a strict default ObjectMapper (FAIL_ON_UNKNOWN defaults to true)"
        UpdatePayload<ArrayList<String>> payload = this.mapper.readValue(
                futureJson, new TypeReference<UpdatePayload<ArrayList<String>>>() {})

        then: "@JsonIgnoreProperties makes the unknown field ignored rather than fatal"
        noExceptionThrown()
        payload.isMetadataOnly()
    }
}
