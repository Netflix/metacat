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
}
