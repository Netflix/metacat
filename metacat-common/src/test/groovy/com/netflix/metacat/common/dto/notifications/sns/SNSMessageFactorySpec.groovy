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
package com.netflix.metacat.common.dto.notifications.sns

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.fge.jsonpatch.diff.JsonDiff
import com.netflix.metacat.common.dto.PartitionDto
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.dto.notifications.sns.messages.*
import com.netflix.metacat.common.dto.notifications.sns.payloads.TablePartitionsUpdatePayload
import com.netflix.metacat.common.dto.notifications.sns.payloads.UpdatePayload
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant

/**
 * Tests for SNSMessageFactory.
 *
 * @author tgianos
 * @since 0.1.47
 */
class SNSMessageFactorySpec extends Specification {
    @Shared
    def mapper = new ObjectMapper()

    def factory = new SNSMessageFactory(this.mapper)


    @Unroll
    def "Can Deserialize #clazz from #message"() {
        when:
        def obj = this.factory.getMessage(message)

        then:
        clazz.isInstance(obj)

        where:
        message | clazz
        this.mapper.writeValueAsString(
            new CreateTableMessage(
                UUID.randomUUID().toString(),
                Instant.now().toEpochMilli(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                new TableDto()
            )
        )       | CreateTableMessage.class
        this.mapper.writeValueAsString(
            new DeleteTableMessage(
                UUID.randomUUID().toString(),
                Instant.now().toEpochMilli(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                new TableDto()
            )
        )       | DeleteTableMessage.class
        this.mapper.writeValueAsString(
            new UpdateTableMessage(
                UUID.randomUUID().toString(),
                Instant.now().toEpochMilli(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                new UpdatePayload<TableDto>(
                    new TableDto(),
                    JsonDiff.asJsonPatch(
                        this.mapper.readTree("{\"a\":\"b\"}"),
                        this.mapper.readTree("{\"a\":\"c\"}")
                    )
                )
            )
        )       | UpdateTableMessage.class
        this.mapper.writeValueAsString(
            new RenameTableMessage(
                UUID.randomUUID().toString(),
                Instant.now().toEpochMilli(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                new UpdatePayload<TableDto>(
                    new TableDto(),
                    JsonDiff.asJsonPatch(
                        this.mapper.readTree("{\"a\":\"b\"}"),
                        this.mapper.readTree("{\"a\":\"c\"}")
                    )
                )
            )
        )       | RenameTableMessage.class
        this.mapper.writeValueAsString(
            new UpdateTablePartitionsMessage(
                UUID.randomUUID().toString(),
                Instant.now().toEpochMilli(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                new TablePartitionsUpdatePayload(
                    UUID.randomUUID().toString(),
                    10,
                    15,
                    "",
                    Arrays.asList(UUID.randomUUID().toString())
                )
            )
        )       | UpdateTablePartitionsMessage.class
        this.mapper.writeValueAsString(
            new AddPartitionMessage(
                UUID.randomUUID().toString(),
                Instant.now().toEpochMilli(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                new PartitionDto()
            )
        )       | AddPartitionMessage.class
        this.mapper.writeValueAsString(
            new DeletePartitionMessage(
                UUID.randomUUID().toString(),
                Instant.now().toEpochMilli(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
            )
        )       | DeletePartitionMessage.class
    }
}
