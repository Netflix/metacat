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
package com.netflix.metacat.common.dto.notifications.sns;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.metacat.common.dto.BaseDto;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import javax.annotation.Nullable;

/**
 * Base SNS notification DTO with shared fields.
 *
 * @param <P> The type of payload this notification has
 * @author tgianos
 * @since 0.1.47
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@SuppressFBWarnings
public class SNSMessage<P> extends BaseDto {
    private final String source = "metacat";
    private final String id;
    private final long timestamp;
    private final String requestId;
    private final String name;
    private final SNSMessageType type;
    private final P payload;

    /**
     * Create a new SNSMessage.
     *
     * @param id        The unique id of the message
     * @param timestamp The number of milliseconds since epoch that this message occurred
     * @param requestId The id of the API request that generated this and possibly other messages. Used for grouping
     * @param type      The type of notification
     * @param name      The qualified name of the resource that this notification is being generated for
     * @param payload   The payload of the notification
     */
    @JsonCreator
    public SNSMessage(
        @JsonProperty("id") @NonNull final String id,
        @JsonProperty("timestamp") final long timestamp,
        @JsonProperty("requestId") @NonNull final String requestId,
        @JsonProperty("type") @NonNull final SNSMessageType type,
        @JsonProperty("name") @NonNull final String name,
        @JsonProperty("payload") @Nullable final P payload
    ) {
        this.id = id;
        this.timestamp = timestamp;
        this.requestId = requestId;
        this.type = type;
        this.name = name;
        this.payload = payload;
    }
}
