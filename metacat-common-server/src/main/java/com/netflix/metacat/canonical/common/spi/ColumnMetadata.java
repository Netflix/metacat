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

/**
 * Created by zhenl on 1/10/17.
 */

import java.util.Objects;

import com.netflix.metacat.canonical.types.Type;

import java.util.Locale;

/**
 * ColumnMetadata class.
 */
public class ColumnMetadata {
    private final String name;
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
    public ColumnMetadata(final String name, final Type type,
                          final boolean partitionKey, final String comment, final boolean hidden) {
        if (name == null || name.isEmpty()) {
            throw new NullPointerException("name is null or empty");
        }
        if (type == null) {
            throw new NullPointerException("type is null");
        }

        this.name = name.toLowerCase(Locale.ENGLISH);
        this.type = type;
        this.partitionKey = partitionKey;
        this.comment = comment;
        this.hidden = hidden;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public boolean isPartitionKey() {
        return partitionKey;
    }

    public String getComment() {
        return comment;
    }

    public boolean isHidden() {
        return hidden;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ColumnMetadata{");
        sb.append("name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", partitionKey=").append(partitionKey);
        if (comment != null) {
            sb.append(", comment='").append(comment).append('\'');
        }
        if (hidden) {
            sb.append(", hidden");
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, partitionKey, comment, hidden);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final ColumnMetadata other = (ColumnMetadata) obj;
        return Objects.equals(this.name, other.name)
            && Objects.equals(this.type, other.type)
            && Objects.equals(this.partitionKey, other.partitionKey)
            && Objects.equals(this.comment, other.comment)
            && Objects.equals(this.hidden, other.hidden);
    }
}
