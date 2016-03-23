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

package com.netflix.metacat.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.metacat.common.dto.PartitionDto;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A fully qualified name that references a source of data
 */
public class QualifiedName implements Serializable {
    private final String catalogName;
    private final String databaseName;
    private final String partitionName;
    private final String tableName;
    private final String viewName;

    private String qualifiedName;
    private Map<String, String> qualifiedNameMap;

    private QualifiedName(
            @Nonnull String catalogName,
            @Nullable String databaseName,
            @Nullable String tableName,
            @Nullable String partitionName,
            @Nullable String viewName
    ) {
        this.catalogName = standardizeRequired("catalogName", catalogName);
        this.databaseName = standardizeOptional(databaseName, true);
        this.tableName = standardizeOptional(tableName, true);
        this.partitionName = standardizeOptional(partitionName, false);
        this.viewName = standardizeOptional(viewName, true);

        if (this.databaseName.isEmpty() && (!this.tableName.isEmpty() || !this.partitionName.isEmpty())) {
            throw new IllegalStateException("databaseName is not present but tableName or partitionName are present");
        } else if (this.tableName.isEmpty() && !this.partitionName.isEmpty()) {
            throw new IllegalStateException("tableName is not present but partitionName is present");
        }
    }


    @JsonCreator
    public static QualifiedName fromJson(JsonNode node) {
        JsonNode catalogNameNode = node.path("catalogName");
        if (catalogNameNode.isMissingNode() || catalogNameNode.isNull() || !catalogNameNode.isTextual()) {
            // If catalogName is not present try to load from the qualifiedName node instead
            JsonNode nameNode = node.path("qualifiedName");
            if (!nameNode.isNull() && nameNode.isTextual()) {
                return fromString(nameNode.asText(), false);
            } else {
                // if neither are available throw an exception
                throw new IllegalStateException("Node '" + node + "' is missing catalogName");
            }
        }
        String catalogName = catalogNameNode.asText();
        JsonNode databaseNameNode = node.path("databaseName");
        String databaseName = null;
        if( databaseNameNode != null){
            databaseName = databaseNameNode.asText();
        }
        JsonNode tableNameNode = node.path("tableName");
        String tableName = null;
        if( tableNameNode != null){
            tableName = tableNameNode.asText();
        }
        JsonNode partitionNameNode = node.path("partitionName");
        String partitionName = null;
        if( partitionNameNode != null){
            partitionName = partitionNameNode.asText();
        }
        JsonNode viewNameNode = node.path("viewName");
        String viewName = null;
        if( viewNameNode != null){
            viewName = viewNameNode.asText();
        }
        return new QualifiedName(catalogName, databaseName, tableName, partitionName, viewName);
    }

    public static QualifiedName fromString(@Nonnull String s){
        return fromString( s, false);
    }

    public static QualifiedName fromString(@Nonnull String s, boolean isView) {
        //noinspection ConstantConditions
        String name = s == null ? "" : s.trim();
        if (name.isEmpty()) {
            throw new IllegalArgumentException("passed in an empty definition name");
        }

        String[] parts = name.split("/", 4);
        switch (parts.length) {
        case 1:
            return ofCatalog(parts[0]);
        case 2:
            return ofDatabase(parts[0], parts[1]);
        case 3:
            return ofTable(parts[0], parts[1], parts[2]);
        case 4:
            if( isView){
                return ofView(parts[0], parts[1], parts[2], parts[3]);
            } else {
                return ofPartition(parts[0], parts[1], parts[2], parts[3]);
            }
        default:
            throw new IllegalArgumentException("Unable to convert '" + s + "' into a qualifiedDefinition");
        }
    }

    public static QualifiedName ofCatalog(@Nonnull String catalogName) {
        return new QualifiedName(catalogName, null, null, null, null);
    }

    public static QualifiedName ofDatabase(@Nonnull String catalogName, @Nonnull String databaseName) {
        return new QualifiedName(catalogName, databaseName, null, null, null);
    }

    public static QualifiedName ofView(@Nonnull String catalogName, @Nonnull String databaseName,
            @Nonnull String tableName, @Nonnull String viewName) {
        return new QualifiedName( catalogName, databaseName, tableName, null, viewName);
    }

    public static QualifiedName ofPartition(@Nonnull QualifiedName tableName, @Nonnull PartitionDto partitionDto) {
        return ofPartition(
                tableName.tableName,
                tableName.databaseName,
                tableName.tableName,
                partitionDto.getName().getPartitionName()
        );
    }

    public static QualifiedName ofPartition(@Nonnull String catalogName, @Nonnull String databaseName,
            @Nonnull String tableName, @Nonnull String partitionName) {
        return new QualifiedName(catalogName, databaseName, tableName, partitionName, null);
    }

    public static QualifiedName ofTable(@Nonnull String catalogName, @Nonnull String databaseName,
            @Nonnull String tableName) {
        return new QualifiedName(catalogName, databaseName, tableName, null, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QualifiedName)) return false;
        QualifiedName that = (QualifiedName) o;
        return Objects.equals(catalogName, that.catalogName) &&
                Objects.equals(databaseName, that.databaseName) &&
                Objects.equals(partitionName, that.partitionName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(viewName, that.viewName);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDatabaseName() {
        if (databaseName.isEmpty()) {
            throw new IllegalStateException("This is not a database definition");
        }
        return databaseName;
    }

    public String getPartitionName() {
        if (partitionName.isEmpty()) {
            throw new IllegalStateException("This is not a partition definition");
        }
        return partitionName;
    }

    public String getTableName() {
        if (tableName.isEmpty()) {
            throw new IllegalStateException("This is not a table definition");
        }
        return tableName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, partitionName, tableName, viewName);
    }

    public boolean isCatalogDefinition() {
        return !catalogName.isEmpty();
    }

    public boolean isDatabaseDefinition() {
        return !databaseName.isEmpty();
    }

    public boolean isPartitionDefinition() {
        return !partitionName.isEmpty();
    }

    public boolean isTableDefinition() {
        return !tableName.isEmpty();
    }

    private String standardizeOptional(String value, boolean forceLowerCase) {
        if (value == null) {
            return "";
        } else {
            value = value.trim();
            if (forceLowerCase) {
                value = value.toLowerCase();
            }
            return value;
        }
    }

    private String standardizeRequired(String name, String value) {
        if (value == null) {
            throw new IllegalStateException(name + " cannot be null");
        }

        value = value.trim();
        if (value.isEmpty()) {
            throw new IllegalStateException(name + " cannot be an empty string");
        }

        return value.toLowerCase();
    }

    @JsonValue
    public Map<String, String> toJson() {
        if( qualifiedNameMap == null) {
            Map<String, String> map = new HashMap<>(4);
            map.put("qualifiedName", toString());
            map.put("catalogName", catalogName);

            if (!databaseName.isEmpty()) {
                map.put("databaseName", databaseName);
            }

            if (!tableName.isEmpty()) {
                map.put("tableName", tableName);
            }

            if (!partitionName.isEmpty()) {
                map.put("partitionName", partitionName);
            }

            if (!viewName.isEmpty()) {
                map.put("viewName", viewName);
            }

            qualifiedNameMap = map;
        }

        return qualifiedNameMap;
    }

    public boolean isViewDefinition() {
        return !viewName.isEmpty();
    }

    @Override
    public String toString() {
        if( qualifiedName == null) {
            StringBuilder sb = new StringBuilder(catalogName);

            if (!databaseName.isEmpty()) {
                sb.append('/');
                sb.append(databaseName);
            }

            if (!tableName.isEmpty()) {
                sb.append('/');
                sb.append(tableName);
            }

            if (!partitionName.isEmpty()) {
                sb.append('/');
                sb.append(partitionName);
            }

            if (!viewName.isEmpty()) {
                sb.append('/');
                sb.append(viewName);
            }
            qualifiedName = sb.toString();
        }

        return qualifiedName;
    }

    public static String toWildCardString(String sourceName, String databaseName, String tableName){
        if( sourceName == null && databaseName ==null && tableName == null){
            return null;
        }
        StringBuilder builder = new StringBuilder();
        if( sourceName != null){
            builder.append(sourceName);
        } else {
            builder.append('%');
        }
        if(databaseName != null){
            builder.append('/').append(databaseName);
        } else {
            builder.append("/%");
        }
        if(tableName !=  null){
            builder.append('/').append(tableName);
        } else {
            builder.append("/%");
        }
        builder.append('%');
        return builder.toString();
    }

    public String getViewName() {
        return viewName;
    }
}
