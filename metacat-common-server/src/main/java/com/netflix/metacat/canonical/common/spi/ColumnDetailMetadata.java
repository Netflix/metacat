/*
 *  Copyright 2016 Netflix, Inc.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *         http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.canonical.common.spi;

import com.google.common.base.Objects;
import com.netflix.metacat.canonical.types.Type;

/**
 * Extended ColumnMetadata class to add more details.
 */
public class ColumnDetailMetadata extends ColumnMetadata {
    private final String sourceType;
    private final Integer size;
    private final Boolean isNullable;
    private final String defaultValue;
    private final Boolean isSortKey;
    private final Boolean isIndexKey;

    /**
     * Constructor.
     * @param name name
     * @param type type
     * @param partitionKey is it a partition key
     * @param sourceType source type
     */
    public ColumnDetailMetadata(final String name, final Type type, final boolean partitionKey,
                                final String sourceType) {
        this(name, type, partitionKey, null, false, sourceType, null, null, null, null, null);
    }

    /**
     * Constructor.
     * @param name name
     * @param type type
     * @param partitionKey is it a partition key
     * @param comment comment
     * @param hidden hidden
     * @param sourceType source type
     */
    public ColumnDetailMetadata(final String name, final Type type, final boolean partitionKey, final String comment,
                                final boolean hidden, final String sourceType) {
        this(name, type, partitionKey, comment, hidden, sourceType, null, null, null, null, null);
    }

    /**
     * Constructor.
     * @param name name
     * @param type type
     * @param partitionKey is it a partition key
     * @param comment comment
     * @param hidden hidden
     * @param sourceType source type
     * @param size size
     * @param isNullable is it nullable
     * @param defaultValue default value
     * @param isSortKey is it an sort column
     * @param isIndexKey is it an index column
     */
    public ColumnDetailMetadata(final String name, final Type type, final boolean partitionKey, final String comment,
                                final boolean hidden, final String sourceType, final Integer size,
                                final Boolean isNullable, final String defaultValue,
                                final Boolean isSortKey, final Boolean isIndexKey) {
        super(name, type, partitionKey, comment, hidden);
        this.sourceType = sourceType;
        this.size = size;
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
        this.isSortKey = isSortKey;
        this.isIndexKey = isIndexKey;
    }

    public String getSourceType() {
        return sourceType;
    }

    public Boolean getIsNullable() {
        return isNullable;
    }

    public Integer getSize() {
        return size;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public Boolean getIsSortKey() {
        return isSortKey;
    }

    public Boolean getIsIndexKey() {
        return isIndexKey;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final ColumnDetailMetadata that = (ColumnDetailMetadata) o;
        return Objects.equal(sourceType, that.sourceType)
            && Objects.equal(size, that.size)
            && Objects.equal(isNullable, that.isNullable)
            && Objects.equal(defaultValue, that.defaultValue)
            && Objects.equal(isSortKey, that.isSortKey)
            && Objects.equal(isIndexKey, that.isIndexKey);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), sourceType, size, isNullable, defaultValue, isSortKey, isIndexKey);
    }
}
