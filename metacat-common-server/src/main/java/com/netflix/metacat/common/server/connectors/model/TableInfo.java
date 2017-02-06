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

import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

/**
 * Table Info.
 *
 * @author tgianos
 * @since 0.1.52
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class TableInfo extends BaseInfo {

    // Required fields
    private final List<FieldInfo> fields;

    // Optional Fields
    private final StorageInfo serde;

    private TableInfo(@Nonnull final Builder builder) {
        super(builder);
        this.fields = builder.fields;
        this.serde = builder.serde;
    }

    public List<FieldInfo> getFields() {
        return Lists.newArrayList(this.fields);
    }

    /**
     * Get the serde used when writing data to this table.
     *
     * @return The serde wrapped in an optional
     */
    public Optional<StorageInfo> getSerde() {
        return Optional.ofNullable(this.serde);
    }

    /**
     * Builder for TableInfo instances.
     *
     * @author tgianos
     * @since 0.1.52
     */
    public static class Builder extends BaseInfo.Builder<Builder> {

        private final List<FieldInfo> fields;
        private StorageInfo serde;

        /**
         * Create a new TableInfo builder.
         *
         * @param fields The fields of the table
         */
        public Builder(@Nonnull final QualifiedName name, @Nonnull final List<FieldInfo> fields) {
            super(name);
            this.fields = Lists.newArrayList(fields);
        }

        /**
         * Set the serde to use with this table.
         *
         * @param serde The serde
         * @return The builder
         */
        public Builder withSerde(@Nullable final StorageInfo serde) {
            this.serde = serde;
            return this;
        }

        /**
         * Create a new immutable instance of a table info object.
         *
         * @return The table info object
         */
        public TableInfo build() {
            return new TableInfo(this);
        }
    }
}
