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
package com.netflix.metacat.common.dto.notifications.sns.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.dto.notifications.sns.SNSMessage;
import com.netflix.metacat.common.dto.notifications.sns.SNSMessageType;
import com.netflix.metacat.common.dto.notifications.sns.payloads.UpdatePayload;
import lombok.Getter;
import lombok.ToString;

/**
 * A message sent when a table is updated.
 *
 * @author tgianos
 * @since 0.1.47
 */
@Getter
@ToString(callSuper = true)
public class UpdateTableMessage extends SNSMessage<UpdatePayload<TableDto>> {

    /**
     * Create a new UpdateTableMessage.
     *
     * @param id        The unique id of the message
     * @param timestamp The number of milliseconds since epoch that this message occurred
     * @param requestId The id of the API request that generated this and possibly other messages. Used for grouping
     * @param name      The qualified name of the resource that this notification is being generated for
     * @param payload   The payload of the notification
     */
    @JsonCreator
    public UpdateTableMessage(
        @JsonProperty("id") final String id,
        @JsonProperty("timestamp") final long timestamp,
        @JsonProperty("requestId") final String requestId,
        @JsonProperty("name") final String name,
        @JsonProperty("payload") final UpdatePayload<TableDto> payload
    ) {
        super(id, timestamp, requestId, SNSMessageType.TABLE_UPDATE, name, payload);
    }
}
