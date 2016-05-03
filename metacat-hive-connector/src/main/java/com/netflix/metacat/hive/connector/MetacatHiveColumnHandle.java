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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;

import java.util.Objects;

import static com.facebook.presto.hive.util.Types.checkType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by amajumdar on 5/2/16.
 */
public class MetacatHiveColumnHandle implements ColumnHandle{
    private final String clientId;
    private final String name;
    private final int ordinalPosition;
    private final Type type;
    private final String hiveTypeName;
    private final int hiveColumnIndex;
    private final boolean partitionKey;

    public MetacatHiveColumnHandle(
            String clientId,
            String name,
            int ordinalPosition,
            String hiveTypeName,
            Type type,
            int hiveColumnIndex,
            boolean partitionKey)
    {
        this.clientId = checkNotNull(clientId, "clientId is null");
        this.name = checkNotNull(name, "name is null");
        checkArgument(ordinalPosition >= 0, "ordinalPosition is negative");
        this.ordinalPosition = ordinalPosition;
        checkArgument(hiveColumnIndex >= 0 || partitionKey, "hiveColumnIndex is negative");
        this.hiveColumnIndex = hiveColumnIndex;
        this.hiveTypeName = checkNotNull(hiveTypeName, "hive type is null");
        this.type = checkNotNull(type, "type is null");
        this.partitionKey = partitionKey;
    }

    public String getClientId()
    {
        return clientId;
    }

    public String getName()
    {
        return name;
    }

    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public int getHiveColumnIndex()
    {
        return hiveColumnIndex;
    }

    public boolean isPartitionKey()
    {
        return partitionKey;
    }

    public ColumnMetadata getColumnMetadata(TypeManager typeManager)
    {
        return new ColumnMetadata(name, type, partitionKey);
    }

    public Type getType()
    {
        return type;
    }

    public String getHiveTypeName() {
        return hiveTypeName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(clientId, name, hiveColumnIndex, partitionKey);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MetacatHiveColumnHandle other = (MetacatHiveColumnHandle) obj;
        return Objects.equals(this.clientId, other.clientId) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.hiveTypeName, other.hiveTypeName) &&
                Objects.equals(this.hiveColumnIndex, other.hiveColumnIndex) &&
                Objects.equals(this.partitionKey, other.partitionKey);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clientId", clientId)
                .add("name", name)
                .add("ordinalPosition", ordinalPosition)
                .add("hiveColumnIndex", hiveColumnIndex)
                .add("hiveTypeName", hiveTypeName)
                .add("partitionKey", partitionKey)
                .toString();
    }

    public static MetacatHiveColumnHandle toHiveColumnHandle(ColumnHandle columnHandle)
    {
        return checkType(columnHandle, MetacatHiveColumnHandle.class, "columnHandle");
    }

}
