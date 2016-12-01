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

package com.netflix.metacat.hive.connector;

import com.facebook.presto.hive.util.Types;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Column handle.
 */
public class MetacatHiveColumnHandle implements ColumnHandle {
    private final String clientId;
    private final String name;
    private final int ordinalPosition;
    private final Type type;
    private final String hiveTypeName;
    private final int hiveColumnIndex;
    private final boolean partitionKey;

    /**
     * Constructor.
     * @param clientId client id
     * @param name name
     * @param ordinalPosition position
     * @param hiveTypeName hive type
     * @param type type
     * @param hiveColumnIndex index
     * @param partitionKey is partition key
     */
    public MetacatHiveColumnHandle(
        final String clientId,
        final String name,
        final int ordinalPosition,
        final String hiveTypeName,
        final Type type,
        final int hiveColumnIndex,
        final boolean partitionKey) {
        this.clientId = Preconditions.checkNotNull(clientId, "clientId is null");
        this.name = Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkArgument(ordinalPosition >= 0, "ordinalPosition is negative");
        this.ordinalPosition = ordinalPosition;
        Preconditions.checkArgument(hiveColumnIndex >= 0 || partitionKey, "hiveColumnIndex is negative");
        this.hiveColumnIndex = hiveColumnIndex;
        this.hiveTypeName = Preconditions.checkNotNull(hiveTypeName, "hive type is null");
        this.type = Preconditions.checkNotNull(type, "type is null");
        this.partitionKey = partitionKey;
    }

    public String getClientId() {
        return clientId;
    }

    public String getName() {
        return name;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public int getHiveColumnIndex() {
        return hiveColumnIndex;
    }

    public boolean isPartitionKey() {
        return partitionKey;
    }

    /**
     * Returns the column metadata.
     * @param typeManager manager
     * @return column
     */
    public ColumnMetadata getColumnMetadata(final TypeManager typeManager) {
        return new ColumnMetadata(name, type, partitionKey);
    }

    public Type getType() {
        return type;
    }

    public String getHiveTypeName() {
        return hiveTypeName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, name, hiveColumnIndex, partitionKey);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final MetacatHiveColumnHandle other = (MetacatHiveColumnHandle) obj;
        return Objects.equals(this.clientId, other.clientId)
            && Objects.equals(this.name, other.name)
            && Objects.equals(this.hiveTypeName, other.hiveTypeName)
            && Objects.equals(this.hiveColumnIndex, other.hiveColumnIndex)
            && Objects.equals(this.partitionKey, other.partitionKey);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("clientId", clientId)
            .add("name", name)
            .add("ordinalPosition", ordinalPosition)
            .add("hiveColumnIndex", hiveColumnIndex)
            .add("hiveTypeName", hiveTypeName)
            .add("partitionKey", partitionKey)
            .toString();
    }

    /**
     * Creates column handle.
     * @param columnHandle column handle
     * @return column handle
     */
    public static MetacatHiveColumnHandle toHiveColumnHandle(final ColumnHandle columnHandle) {
        return Types.checkType(columnHandle, MetacatHiveColumnHandle.class, "columnHandle");
    }

}
