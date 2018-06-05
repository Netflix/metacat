/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.metacat.common.dto.PartitionDto;
import lombok.Getter;
import lombok.NonNull;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A fully qualified name that references a source of data.
 *
 * @author amajumdar
 */
@Getter
public final class QualifiedName implements Serializable {
    private final String catalogName;
    private final String databaseName;
    private final String partitionName;
    private final String tableName;
    private final String viewName;
    private String qualifiedName;
    private Map<String, String> qualifiedNameMap;
    private Map<String, String> parts;
    private Type type;

    private QualifiedName(
        @NonNull final String catalogName,
        @Nullable final String databaseName,
        @Nullable final String tableName,
        @Nullable final String partitionName,
        @Nullable final String viewName
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

        if (!this.viewName.isEmpty()) {
            type = Type.MVIEW;
        } else if (!this.partitionName.isEmpty()) {
            type = Type.PARTITION;
        } else if (!this.tableName.isEmpty()) {
            type = Type.TABLE;
        } else if (!this.databaseName.isEmpty()) {
            type = Type.DATABASE;
        } else {
            type = Type.CATALOG;
        }
    }

    private QualifiedName(
        @NonNull final String catalogName,
        @Nullable final String databaseName,
        @Nullable final String tableName,
        @Nullable final String partitionName,
        @Nullable final String viewName,
        @NonNull final Type type) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.partitionName = partitionName;
        this.tableName = tableName;
        this.viewName = viewName;
        this.type = type;
    }

    /**
     * Creates the name from the json.
     *
     * @param node json node
     * @return qualified name
     */
    @JsonCreator
    public static QualifiedName fromJson(@NonNull final JsonNode node) {
        final JsonNode catalogNameNode = node.path("catalogName");
        if (catalogNameNode.isMissingNode() || catalogNameNode.isNull() || !catalogNameNode.isTextual()) {
            // If catalogName is not present try to load from the qualifiedName node instead
            final JsonNode nameNode = node.path("qualifiedName");
            if (!nameNode.isNull() && nameNode.isTextual()) {
                return fromString(nameNode.asText(), false);
            } else {
                // if neither are available throw an exception
                throw new IllegalStateException("Node '" + node + "' is missing catalogName");
            }
        }
        final String catalogName = catalogNameNode.asText();
        final JsonNode databaseNameNode = node.path("databaseName");
        String databaseName = null;
        if (databaseNameNode != null) {
            databaseName = databaseNameNode.asText();
        }
        final JsonNode tableNameNode = node.path("tableName");
        String tableName = null;
        if (tableNameNode != null) {
            tableName = tableNameNode.asText();
        }
        final JsonNode partitionNameNode = node.path("partitionName");
        String partitionName = null;
        if (partitionNameNode != null) {
            partitionName = partitionNameNode.asText();
        }
        final JsonNode viewNameNode = node.path("viewName");
        String viewName = null;
        if (viewNameNode != null) {
            viewName = viewNameNode.asText();
        }
        return new QualifiedName(catalogName, databaseName, tableName, partitionName, viewName);
    }

    /**
     * Creates the qualified name from text.
     *
     * @param s name
     * @return qualified name
     */
    public static QualifiedName fromString(@NonNull final String s) {
        return fromString(s, false);
    }

    /**
     * Creates the qualified name from text.
     *
     * @param s      name
     * @param isView true if it represents a view
     * @return qualified name
     */
    public static QualifiedName fromString(@NonNull final String s, final boolean isView) {
        //noinspection ConstantConditions
        final String name = s.trim();
        if (name.isEmpty()) {
            throw new IllegalArgumentException("passed in an empty definition name");
        }

        final String[] parts = name.split("/", 4);
        switch (parts.length) {
            case 1:
                return ofCatalog(parts[0]);
            case 2:
                return ofDatabase(parts[0], parts[1]);
            case 3:
                return ofTable(parts[0], parts[1], parts[2]);
            case 4:
                if (isView || !parts[3].contains("=")) {
                    return ofView(parts[0], parts[1], parts[2], parts[3]);
                } else {
                    return ofPartition(parts[0], parts[1], parts[2], parts[3]);
                }
            default:
                throw new IllegalArgumentException("Unable to convert '" + s + "' into a qualifiedDefinition");
        }
    }

    /**
     * Returns a copy of this qualified name with the database/table/view names in upper case.
     *
     * @return QualifiedName
     */
    public QualifiedName cloneWithUpperCase() {
        return new QualifiedName(catalogName,
            databaseName == null ? null : databaseName.toUpperCase(),
            tableName == null ? null : tableName.toUpperCase(),
            partitionName,
            viewName == null ? null : viewName.toUpperCase(),
            type);
    }

    /**
     * Creates the qualified name representing a catalog.
     *
     * @param catalogName catalog name
     * @return qualified name
     */
    public static QualifiedName ofCatalog(@NonNull final String catalogName) {
        return new QualifiedName(catalogName, null, null, null, null);
    }

    /**
     * Creates the qualified name representing a database.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @return qualified name
     */
    public static QualifiedName ofDatabase(
        @NonNull final String catalogName,
        @NonNull final String databaseName
    ) {
        return new QualifiedName(catalogName, databaseName, null, null, null);
    }

    /**
     * Creates the qualified name representing a view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @return qualified name
     */
    public static QualifiedName ofView(
        @NonNull final String catalogName,
        @NonNull final String databaseName,
        @NonNull final String tableName,
        @NonNull final String viewName
    ) {
        return new QualifiedName(catalogName, databaseName, tableName, null, viewName);
    }

    /**
     * Creates the qualified name representing a partition.
     *
     * @param tableName    table name
     * @param partitionDto partition
     * @return qualified name
     */
    public static QualifiedName ofPartition(
        @NonNull final QualifiedName tableName,
        @NonNull final PartitionDto partitionDto
    ) {
        return ofPartition(
            tableName.catalogName,
            tableName.databaseName,
            tableName.tableName,
            partitionDto.getName().getPartitionName()
        );
    }

    /**
     * Creates the qualified name representing a partition.
     *
     * @param catalogName   catalog name
     * @param databaseName  database name
     * @param tableName     table name
     * @param partitionName partition name
     * @return qualified name
     */
    public static QualifiedName ofPartition(
        @NonNull final String catalogName,
        @NonNull final String databaseName,
        @NonNull final String tableName,
        @NonNull final String partitionName
    ) {
        return new QualifiedName(catalogName, databaseName, tableName, partitionName, null);
    }

    /**
     * Creates the qualified name representing a table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return qualified name
     */
    public static QualifiedName ofTable(
        @NonNull final String catalogName,
        @NonNull final String databaseName,
        @NonNull final String tableName
    ) {
        return new QualifiedName(catalogName, databaseName, tableName, null, null);
    }

    /**
     * Creates a wild card string format of the qualified name.
     *
     * @param sourceName   catalog/source name
     * @param databaseName database name
     * @param tableName    table name
     * @return wild card string format of the qualified name
     */
    public static String toWildCardString(
        @Nullable final String sourceName,
        @Nullable final String databaseName,
        @Nullable final String tableName
    ) {
        if (sourceName == null && databaseName == null && tableName == null) {
            return null;
        }
        final StringBuilder builder = new StringBuilder();
        if (sourceName != null) {
            builder.append(sourceName);
        } else {
            builder.append('%');
        }
        if (databaseName != null) {
            builder.append('/').append(databaseName);
        } else {
            builder.append("/%");
        }
        if (tableName != null) {
            builder.append('/').append(tableName);
        } else {
            builder.append("/%");
        }
        builder.append('%');
        return builder.toString();
    }

    /**
     * Get the catalog name.
     *
     * @return The catalog name
     */
    public String getCatalogName() {
        return this.catalogName;
    }

    /**
     * Returns the database name.
     *
     * @return database name
     */
    public String getDatabaseName() {
        // TODO: This is a bad exception to throw. If its truly an illegal state exception we shouldn't allow that
        //       object to be built.
        if (this.databaseName.isEmpty()) {
            throw new IllegalStateException("This is not a database definition");
        }
        return this.databaseName;
    }

    /**
     * Returns the partition name.
     *
     * @return partition name
     */
    public String getPartitionName() {
        // TODO: This is a bad exception to throw. If its truly an illegal state exception we shouldn't allow that
        //       object to be built.
        if (partitionName.isEmpty()) {
            throw new IllegalStateException("This is not a partition definition");
        }
        return partitionName;
    }

    /**
     * Returns the table name.
     *
     * @return table name
     */
    public String getTableName() {
        // TODO: This is a bad exception to throw. If its truly an illegal state exception we shouldn't allow that
        //       object to be built.
        if (tableName.isEmpty()) {
            throw new IllegalStateException("This is not a table definition");
        }
        return tableName;
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

    private String standardizeOptional(@Nullable final String value, final boolean forceLowerCase) {
        if (value == null) {
            return "";
        } else {
            String returnValue = value.trim();
            if (forceLowerCase) {
                returnValue = returnValue.toLowerCase();
            }
            return returnValue;
        }
    }

    private String standardizeRequired(final String name, @Nullable final String value) {
        if (value == null) {
            throw new IllegalStateException(name + " cannot be null");
        }

        final String returnValue = value.trim();
        if (returnValue.isEmpty()) {
            throw new IllegalStateException(name + " cannot be an empty string");
        }

        return returnValue.toLowerCase();
    }

    /**
     * Returns the qualified name in the JSON format.
     *
     * @return qualified name
     */
    @JsonValue
    public Map<String, String> toJson() {
        if (qualifiedNameMap == null) {
            final Map<String, String> map = new HashMap<>(5);
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

    /**
     * Returns the qualified name in parts.
     *
     * @return parts of the qualified name as a Map
     */
    public Map<String, String> parts() {
        if (parts == null) {
            final Map<String, String> map = new HashMap<>(4);
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

            parts = map;
        }

        return parts;
    }

    public boolean isViewDefinition() {
        return !viewName.isEmpty();
    }

    // TODO: Replace custom equals and hashcode with generated. Tried but broke tests.

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QualifiedName)) {
            return false;
        }
        final QualifiedName that = (QualifiedName) o;
        return Objects.equals(catalogName, that.catalogName)
            && Objects.equals(databaseName, that.databaseName)
            && Objects.equals(partitionName, that.partitionName)
            && Objects.equals(tableName, that.tableName)
            && Objects.equals(viewName, that.viewName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, partitionName, tableName, viewName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        if (qualifiedName == null) {
            final StringBuilder sb = new StringBuilder(catalogName);

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

    /**
     * Type of the connector resource.
     */
    public enum Type {
        /**
         * Catalog type.
         */
        CATALOG,

        /**
         * Database type.
         */
        DATABASE,

        /**
         * Table type.
         */
        TABLE,

        /**
         * Partition type.
         */
        PARTITION,

        /**
         * MView type.
         */
        MVIEW
    }
}
