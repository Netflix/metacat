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
package com.netflix.metacat.common.dto.notifications.sns.payloads;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.fge.jsonpatch.JsonPatch;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Represents the contents of an update payload.
 *
 * @param <T> The DTO type that was update. e.g. com.netflix.metacat.common.dto.TableDto
 * @author tgianos
 * @since 0.1.47
 */
@Getter
@ToString
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class UpdatePayload<T> {
    private T previous;
    private JsonPatch patch;

    /**
     * Whether the update changed only table metadata (no new Iceberg snapshot). See
     * {@code MetacatUpdateTablePostEvent#getMetadataOnlyUpdate()} for the exact semantics; {@code false}
     * means either a data change or that it could not be determined.
     */
    private boolean metadataOnly;

    /**
     * Create a new update payload without a metadata-only indicator (defaults to {@code false}).
     *
     * @param previous The previous version of the object that was updated
     * @param patch    The JSON patch to go from previous to current
     */
    public UpdatePayload(final T previous, final JsonPatch patch) {
        this(previous, patch, false);
    }

    /**
     * Create a new update payload.
     *
     * @param previous     The previous version of the object that was updated
     * @param patch        The JSON patch to go from previous to current
     * @param metadataOnly Whether the update changed only table metadata; {@code false} if not determinable
     */
    @JsonCreator
    public UpdatePayload(
            @JsonProperty("previous") final T previous,
            @JsonProperty("patch") final JsonPatch patch,
            @JsonProperty("metadataOnly") final boolean metadataOnly
    ) {
        this.previous = previous;
        this.patch = patch;
        this.metadataOnly = metadataOnly;
    }
}
