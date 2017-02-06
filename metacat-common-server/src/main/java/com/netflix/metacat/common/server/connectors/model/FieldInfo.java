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

import com.netflix.metacat.common.type.Type;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * The field of a table in a database.
 *
 * @author tgianos
 * @since 0.1.52
 */
@ToString
@EqualsAndHashCode
public class FieldInfo {
    // Required fields
    @Getter
    private final String name;
    @Getter
    private final String sourceType;
    @Getter
    private final Type type;

    // Optional fields
    private final String comment;
    private final Boolean partitionKey;
    private final Integer pos;
    private final Boolean nullable;
    private final Integer size;
    private final String defaultValue;
    private final Boolean sortKey;
    private final Boolean indexKey;

    private FieldInfo(final Builder builder) {
        this.name = builder.name;
        this.sourceType = builder.sourceType;
        this.type = builder.type;

        this.comment = builder.comment;
        this.partitionKey = builder.partitionKey;
        this.pos = builder.pos;
        this.nullable = builder.nullable;
        this.size = builder.size;
        this.defaultValue = builder.defaultValue;
        this.sortKey = builder.sortKey;
        this.indexKey = builder.indexKey;
    }

    /**
     * Get the comment associated with this field.
     *
     * @return Optional of the comment
     */
    public Optional<String> getComment() {
        return Optional.ofNullable(this.comment);
    }

    /**
     * Whether this field is a partition key or not. May not apply to certain connector types.
     *
     * @return Optional if it is a partition key or not.
     */
    public Optional<Boolean> isPartitionKey() {
        return Optional.ofNullable(this.partitionKey);
    }

    /**
     * Get the position of this field within a row record.
     *
     * @return Optional of the position
     */
    public Optional<Integer> getPos() {
        return Optional.ofNullable(this.pos);
    }

    /**
     * Whether this field is nullable or not. May not be relevant for certain connectors.
     *
     * @return Optional of nullable value
     */
    public Optional<Boolean> isNullable() {
        return Optional.ofNullable(this.nullable);
    }

    /**
     * Get the size of this field. May not be relavant for certain containers.
     *
     * @return Optional of the size
     */
    public Optional<Integer> getSize() {
        return Optional.ofNullable(this.size);
    }

    /**
     * Get the default value of this field.
     *
     * @return The default value
     */
    public Optional<String> getDefaultValue() {
        return Optional.ofNullable(this.defaultValue);
    }

    /**
     * Whether or not this field is a sort key.
     *
     * @return Optional of a boolean
     */
    public Optional<Boolean> isSortKey() {
        return Optional.ofNullable(this.sortKey);
    }

    /**
     * Whether or not this field is an index key.
     *
     * @return Optional of a boolean
     */
    public Optional<Boolean> isIndexKey() {
        return Optional.ofNullable(this.indexKey);
    }

    /**
     * Builder for immutable instances of FieldInfo.
     *
     * @author tgianos
     * @since 0.1.52
     */
    public static class Builder {
        private final String name;
        private final String sourceType;
        private final Type type;

        private String comment;
        private Boolean partitionKey;
        private Integer pos;
        private Boolean nullable;
        private Integer size;
        private String defaultValue;
        private Boolean sortKey;
        private Boolean indexKey;

        /**
         * Constructor with required fields.
         *
         * @param name       The name of the field
         * @param sourceType The type in the source data repository
         * @param type       The metacat type
         */
        public Builder(@Nonnull final String name, @Nonnull final String sourceType, @Nonnull final Type type) {
            this.name = name;
            this.sourceType = sourceType;
            this.type = type;
        }

        /**
         * Set the comment to use for this field.
         *
         * @param comment The optional comment
         * @return The builder
         */
        public Builder withComment(@Nullable final String comment) {
            this.comment = comment;
            return this;
        }

        /**
         * Set whether this field is a partition key or not.
         *
         * @param partitionKey True if this is a partition key
         * @return The builder
         */
        public Builder withPartitionKey(@Nullable final Boolean partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        /**
         * Set the position this field occurs at in a record.
         *
         * @param pos The position
         * @return The builder
         */
        public Builder withPos(@Nullable final Integer pos) {
            this.pos = pos;
            return this;
        }

        /**
         * Set whether this field is nullable or not.
         *
         * @param nullable true if the field can be null
         * @return The builder
         */
        public Builder withNullable(@Nullable final Boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        /**
         * The size of the field. e.g. VARCHAR(11) size would be 11
         *
         * @param size The size of the field
         * @return The builder
         */
        public Builder withSize(@Nullable final Integer size) {
            this.size = size;
            return this;
        }

        /**
         * The default value of the field.
         *
         * @param defaultValue The default value the field should have
         * @return The builder
         */
        public Builder withDefaultValue(@Nullable final String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        /**
         * Whether or not this field is a sort key.
         *
         * @param sortKey True if it is a sort key
         * @return The builder
         */
        public Builder withSortKey(@Nullable final Boolean sortKey) {
            this.sortKey = sortKey;
            return this;
        }

        /**
         * Whether or not this field is an index key.
         *
         * @param indexKey True if the field is an index key
         * @return The builder
         */
        public Builder withIndexKey(@Nullable final Boolean indexKey) {
            this.indexKey = indexKey;
            return this;
        }

        /**
         * Create an immutable instance of FieldInfo out of the contents of this builder.
         *
         * @return The immutable FieldInfo instance
         */
        public FieldInfo build() {
            return new FieldInfo(this);
        }
    }
}
