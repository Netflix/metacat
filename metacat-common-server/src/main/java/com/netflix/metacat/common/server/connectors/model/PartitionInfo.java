/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.metacat.common.server.connectors.model;

import com.netflix.metacat.common.QualifiedName;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Partition info from connectors.
 *
 * @author tgianos
 * @since 0.1.52
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class PartitionInfo extends BaseInfo {

    private final StorageInfo serde;

    private PartitionInfo(final Builder builder) {
        super(builder);
        this.serde = builder.serde;
    }

    /**
     * Get the serde used when writing data to this partition.
     *
     * @return The serde wrapped in an optional
     */
    public Optional<StorageInfo> getSerde() {
        return Optional.ofNullable(this.serde);
    }

    public static class Builder extends BaseInfo.Builder<Builder> {

        private StorageInfo serde;

        /**
         * Create a new PartitionInfo builder.
         *
         * @param name The qualified name of the partition
         */
        public Builder(@Nonnull final QualifiedName name) {
            super(name);
        }

        /**
         * Set the serde for this partition.
         *
         * @param serde The serde to use
         * @return The builder
         */
        public Builder withSerde(@Nullable final StorageInfo serde) {
            this.serde = serde;
            return this;
        }

        public PartitionInfo build() {
            return new PartitionInfo(this);
        }
    }
}
