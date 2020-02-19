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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.metacat.common.dto.notifications.sns.messages.AddPartitionMessage;
import com.netflix.metacat.common.dto.notifications.sns.messages.CreateTableMessage;
import com.netflix.metacat.common.dto.notifications.sns.messages.DeletePartitionMessage;
import com.netflix.metacat.common.dto.notifications.sns.messages.DeleteTableMessage;
import com.netflix.metacat.common.dto.notifications.sns.messages.RenameTableMessage;
import com.netflix.metacat.common.dto.notifications.sns.messages.UpdateTableMessage;
import com.netflix.metacat.common.dto.notifications.sns.messages.UpdateTablePartitionsMessage;
import lombok.NonNull;

import java.io.IOException;

/**
 * Create SNSMessage object based on the JSON passed in.
 *
 * @author tgianos
 * @since 0.1.47
 */
public class SNSMessageFactory {

    private static final String TYPE_FIELD = "type";
    private final ObjectMapper mapper;

    /**
     * Constructor.
     *
     * @param mapper The object mapper to use for deserialization
     */
    public SNSMessageFactory(@NonNull final ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Convert a JSON String into a message if possible.
     *
     * @param json The body of the message to convert back to the original message object from JSON string
     * @return The message bound back into a POJO
     * @throws IOException When the input isn't valid JSON
     */
    public SNSMessage<?> getMessage(@NonNull final String json) throws IOException {
        final JsonNode object = this.mapper.readTree(json);
        if (object.has(TYPE_FIELD)) {
            final SNSMessageType messageType = SNSMessageType.valueOf(object.get(TYPE_FIELD).asText());
            switch (messageType) {
                case TABLE_CREATE:
                    return this.mapper.readValue(json, CreateTableMessage.class);
                case TABLE_DELETE:
                    return this.mapper.readValue(json, DeleteTableMessage.class);
                case TABLE_UPDATE:
                    return this.mapper.readValue(json, UpdateTableMessage.class);
                case TABLE_RENAME:
                    return this.mapper.readValue(json, RenameTableMessage.class);
                case TABLE_PARTITIONS_UPDATE:
                    return this.mapper.readValue(json, UpdateTablePartitionsMessage.class);
                case PARTITION_ADD:
                    return this.mapper.readValue(json, AddPartitionMessage.class);
                case PARTITION_DELETE:
                    return this.mapper.readValue(json, DeletePartitionMessage.class);
                default:
                    throw new UnsupportedOperationException("Unknown type " + messageType);
            }
        } else {
            // won't know how to bind
            throw new IOException("Invalid content. No field type field found");
        }
    }
}
