/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.common.usermetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.dto.HasDataMetadata;
import com.netflix.metacat.common.dto.HasDefinitionMetadata;
import com.netflix.metacat.common.dto.HasMetadata;

import java.util.Optional;

/**
 * Base class for UserMetadataService.
 * @author amajumdar
 */
public abstract class BaseUserMetadataService implements UserMetadataService {
    /**
     * Saves user metadata.
     * @param userId user name
     * @param holder metadata
     * @param merge true if the metadata should be merged with existing metadata
     */
    public void saveMetadata(final String userId, final HasMetadata holder, final boolean merge) {
        if (holder instanceof HasDefinitionMetadata) {
            final HasDefinitionMetadata defDto = (HasDefinitionMetadata) holder;

            // If the user is updating the definition metadata do a merge on the existing metadata
            final ObjectNode newMetadata = defDto.getDefinitionMetadata();
            if (newMetadata != null) {
                saveDefinitionMetadata(defDto.getDefinitionName(), userId, Optional.of(newMetadata), merge);
            }
        }

        if (holder instanceof HasDataMetadata) {
            final HasDataMetadata dataDto = (HasDataMetadata) holder;

            // If the user is updating the data metadata and a separate data location exists,
            // do a merge on the existing metadata
            final ObjectNode newMetadata = dataDto.getDataMetadata();
            if (newMetadata != null && dataDto.isDataExternal()) {
                saveDataMetadata(dataDto.getDataUri(), userId, Optional.of(newMetadata), merge);
            }
        }
    }

    /**
     * Populate the given metadata.
     * @param holder metadata
     */
    public void populateMetadata(final HasMetadata holder) {
        Optional<ObjectNode> metadata = Optional.empty();
        if (holder instanceof HasDataMetadata) {
            final HasDataMetadata dataDto = (HasDataMetadata) holder;
            if (dataDto.isDataExternal()) {
                metadata = getDataMetadata(dataDto.getDataUri());
            }
        }
        Optional<ObjectNode> definitionMetadata = Optional.empty();
        if (holder instanceof HasDefinitionMetadata) {
            final HasDefinitionMetadata definitionDto = (HasDefinitionMetadata) holder;
            definitionMetadata = getDefinitionMetadata(definitionDto.getDefinitionName());
        }
        populateMetadata(holder, definitionMetadata.orElse(null), metadata.orElse(null));
    }

    /**
     * Populate metadata.
     * @param holder metadata
     * @param definitionMetadata definition metadata
     * @param dataMetadata data metadata
     */
    public void populateMetadata(final HasMetadata holder, final ObjectNode definitionMetadata,
        final ObjectNode dataMetadata) {
        if (holder instanceof HasDefinitionMetadata) {
            final HasDefinitionMetadata defDto = (HasDefinitionMetadata) holder;
            defDto.setDefinitionMetadata(definitionMetadata);
        }

        if (holder instanceof HasDataMetadata) {
            final HasDataMetadata dataDto = (HasDataMetadata) holder;
            if (dataDto.isDataExternal()) {
                dataDto.setDataMetadata(dataMetadata);
            }
        }
    }
}
