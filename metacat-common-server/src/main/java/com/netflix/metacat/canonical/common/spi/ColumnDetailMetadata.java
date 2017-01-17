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

import com.netflix.metacat.canonical.types.Type;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Extended ColumnMetadata class to add more details.
 * @author zhenl
 */
@Getter
@EqualsAndHashCode(callSuper = true)
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
}
