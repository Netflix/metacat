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
package com.netflix.metacat.common.dto.notifications.sns.messages

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.dto.notifications.sns.SNSMessageType
import spock.lang.Specification

import java.time.Instant

/**
 * Tests for CreateTableMessage.
 *
 * @author tgianos
 * @since 0.1.47
 */
class CreateTableMessageSpec extends Specification {

    def mapper = new ObjectMapper()

    def "Can Construct"() {
        def id = UUID.randomUUID().toString()
        def timestamp = Instant.now().toEpochMilli()
        def requestId = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def payload = Mock(TableDto)

        when:
        def message = new CreateTableMessage(id, timestamp, requestId, name, payload)

        then:
        message != null
        message.getId() == id
        message.getTimestamp() == timestamp
        message.getRequestId() == requestId
        message.getName() == name
        message.getType() == SNSMessageType.TABLE_CREATE
        message.getPayload() == payload
    }

    def "Can Serialize and Deserialize"() {
        def id = UUID.randomUUID().toString()
        def timestamp = Instant.now().toEpochMilli()
        def requestId = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()
        def payload = new TableDto()
        def message = new CreateTableMessage(id, timestamp, requestId, name, payload)

        when:
        def json = this.mapper.writeValueAsString(message)
        def message2 = this.mapper.readValue(json, CreateTableMessage.class)

        then:
        message.getId() == message2.getId()
        message.getTimestamp() == message2.getTimestamp()
        message.getRequestId() == message2.getRequestId()
        message.getName() == message2.getName()
        message.getType() == message2.getType()
        message.getPayload() == message2.getPayload()
    }
}
