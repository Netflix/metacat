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

package com.facebook.presto.plugin;

import com.facebook.presto.spi.ColumnDetailMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Column detail handle.
 */
public class ColumnDetailHandle implements ColumnHandle {
    private final String connectorId;
    private final String columnName;
    private final Type columnType;
    private final Boolean isPartitionKey;
    private final String comment;
    private final String sourceType;
    private final Integer size;
    private final Boolean isNullable;
    private final String defaultValue;
    private final Boolean isSortKey;
    private final Boolean isIndexKey;

    /**
     * Constructor.
     * @param connectorId connector id
     * @param columnName column name
     * @param columnType column type
     * @param isPartitionKey is the column a partition key
     * @param comment comment
     * @param sourceType source type
     * @param size size
     * @param isNullable can it be null
     * @param defaultValue default value
     * @param isSortKey is it a sort column
     * @param isIndexKey is it an index column
     */
    @JsonCreator
    public ColumnDetailHandle(
        @JsonProperty("connectorId")
        final String connectorId,
        @JsonProperty("columnName")
        final String columnName,
        @JsonProperty("columnType")
        final Type columnType,
        @JsonProperty("columnType")
        final Boolean isPartitionKey,
        @JsonProperty("columnType")
        final String comment,
        @JsonProperty("sourceType")
        final String sourceType,
        @JsonProperty("size")
        final Integer size,
        @JsonProperty("isNullable")
        final Boolean isNullable,
        @JsonProperty("defaultValue")
        final String defaultValue,
        @JsonProperty("isSortKey")
        final Boolean isSortKey,
        @JsonProperty("isIndexKey")
        final Boolean isIndexKey) {
        this.connectorId = Preconditions.checkNotNull(connectorId, "connectorId is null");
        this.columnName = Preconditions.checkNotNull(columnName, "columnName is null");
        this.columnType = Preconditions.checkNotNull(columnType, "columnType is null");
        this.isPartitionKey = isPartitionKey;
        this.comment = comment;
        this.sourceType = sourceType;
        this.size = size;
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
        this.isSortKey = isSortKey;
        this.isIndexKey = isIndexKey;
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType() {
        return columnType;
    }

    @JsonProperty
    public Boolean getIsPartitionKey() {
        return isPartitionKey;
    }

    @JsonProperty
    public String getComment() {
        return comment;
    }

    @JsonProperty
    public String getSourceType() {
        return sourceType;
    }

    @JsonProperty
    public Integer getSize() {
        return size;
    }

    @JsonProperty
    public Boolean getIsNullable() {
        return isNullable;
    }

    @JsonProperty
    public String getDefaultValue() {
        return defaultValue;
    }

    @JsonProperty
    public Boolean getIsSortKey() {
        return isSortKey;
    }

    @JsonProperty
    public Boolean getIsIndexKey() {
        return isIndexKey;
    }

    /**
     * Returns the column metadata.
     * @return column metadata
     */
    public ColumnMetadata getColumnMetadata() {
        final StringBuilder comments = new StringBuilder(comment == null ? "" : comment).append(" ")
            .append("nullable=").append(isNullable).append(", ")
            .append("columnLength=").append(size).append(", ")
            .append("default=").append(defaultValue).append(", ")
            .append("sortKey=").append(isSortKey).append(", ")
            .append("indexKey=").append(isIndexKey);

        return new ColumnDetailMetadata(columnName, columnType, isPartitionKey == null || isPartitionKey,
            comments.toString(), false, sourceType, size, isNullable, defaultValue, isSortKey, isIndexKey);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        final ColumnDetailHandle o = (ColumnDetailHandle) obj;
        return Objects.equals(this.connectorId, o.connectorId)
            && Objects.equals(this.columnName, o.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, columnName);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("connectorId", connectorId)
            .add("columnName", columnName)
            .add("columnType", columnType)
            .toString();
    }
}
