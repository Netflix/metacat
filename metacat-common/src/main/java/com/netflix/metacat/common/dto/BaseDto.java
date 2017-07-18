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
package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import lombok.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Base class for all common DTOs.
 *
 * @author amajumdar
 */
//TODO: All DTO's should be READ-ONLY
public abstract class BaseDto implements Serializable {
    protected static final MetacatJsonLocator METACAT_JSON_LOCATOR = new MetacatJsonLocator();
    /**
     * Deserialize the input stream.
     *
     * @param inputStream input stream
     * @return Json node
     * @throws IOException exception deserializing the stream
     */
    @Nullable
    public static ObjectNode deserializeObjectNode(
        @Nonnull @NonNull final ObjectInputStream inputStream
    ) throws IOException {
        return METACAT_JSON_LOCATOR.deserializeObjectNode(inputStream);
    }

    /**
     * Serialize the stream.
     *
     * @param outputStream output stream
     * @param json         Json Node
     * @throws IOException exception serializing the json
     */
    public static void serializeObjectNode(
        @Nonnull @NonNull final ObjectOutputStream outputStream,
        @Nullable final ObjectNode json
    ) throws IOException {
        METACAT_JSON_LOCATOR.serializeObjectNode(outputStream, json);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return METACAT_JSON_LOCATOR.toJsonString(this);
    }
}
