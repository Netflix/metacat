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

import javax.annotation.Nonnull;

/**
 * Marker interface for objects with data metadata.
 */
public interface HasDataMetadata extends HasMetadata {
    /**
     * Returns data metadata.
     *
     * @return data metadata
     */
    ObjectNode getDataMetadata();

    /**
     * Sets the data metadata json.
     *
     * @param metadata data metadata json
     */
    void setDataMetadata(ObjectNode metadata);

    /**
     * Returns uri.
     *
     * @return The uri that points to the location of the external data.
     * @throws IllegalStateException if this instance does not have external data
     */
    @Nonnull
    String getDataUri();

    /**
     * Returns true if this particular instance points to external data.
     *
     * @return true if this particular instance points to external data
     */
    boolean isDataExternal();
}
