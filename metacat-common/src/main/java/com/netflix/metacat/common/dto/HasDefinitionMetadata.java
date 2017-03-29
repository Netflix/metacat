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
import com.netflix.metacat.common.QualifiedName;

/**
 * Marker interface for objects with data metadata.
 */
public interface HasDefinitionMetadata extends HasMetadata {
    /**
     * Returns definition metadata.
     *
     * @return definition metadata
     */
    ObjectNode getDefinitionMetadata();

    /**
     * Sets definition metadata.
     *
     * @param metadata definition metadata
     */
    void setDefinitionMetadata(ObjectNode metadata);

    /**
     * Returns the qualified name.
     *
     * @return qualified name
     */
    QualifiedName getDefinitionName();
}
