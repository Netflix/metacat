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

import com.facebook.presto.hadoop.shaded.com.google.common.base.Strings;
import com.google.common.base.Preconditions;
import com.netflix.metacat.canonical.types.Type;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.Locale;

/**
 * ColumnMetadata class.
 */
@Getter
@EqualsAndHashCode
@ToString
public class ColumnMetadata {
    @NonNull
    private final String name;
    @NonNull
    private final Type type;
    private final boolean partitionKey;
    private final String comment;
    private final boolean hidden;

    /**
     * Constructor.
     *
     * @param name         name
     * @param type         type
     * @param partitionKey partitionKey
     */
    public ColumnMetadata(final String name, final Type type, final boolean partitionKey) {
        this(name, type, partitionKey, null, false);
    }

    /**
     * Constructor.
     *
     * @param name         name
     * @param type         type
     * @param partitionKey partitionKey
     * @param comment      comment
     * @param hidden       hidden
     */
    public ColumnMetadata(final String name, @NonNull final Type type,
                          final boolean partitionKey, final String comment, final boolean hidden) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "name is null or empty");
        this.name = name.toLowerCase(Locale.ENGLISH);
        this.type = type;
        this.partitionKey = partitionKey;
        this.comment = comment;
        this.hidden = hidden;
    }
}
