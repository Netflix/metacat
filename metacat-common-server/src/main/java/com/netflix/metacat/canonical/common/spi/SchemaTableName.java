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


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Locale;

/**
 * schemaTableName class.
 */
@Getter
@EqualsAndHashCode
public class SchemaTableName {
    private final String schemaName;
    private final String tableName;

    /**
     * Constructor.
     *
     * @param schemaName schemaName
     * @param tableName  tableName
     */
    public SchemaTableName(final String schemaName, final String tableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(schemaName), "schemaName");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName");
        this.schemaName = schemaName.toLowerCase(Locale.ENGLISH);
        this.tableName = tableName.toLowerCase(Locale.ENGLISH);
    }

    /**
     * vaule of.
     *
     * @param schemaTableName schema table name
     * @return table name
     */
    @JsonCreator
    public static SchemaTableName valueOf(final String schemaTableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(schemaTableName), "schemaTableName");
        final String[] parts = schemaTableName.split("\\.");
        Preconditions.checkArgument(parts.length == 2, "Invalid schemaTableName " + schemaTableName);
        return new SchemaTableName(parts[0], parts[1]);
    }

    @JsonValue
    @Override
    public String toString() {
        return schemaName + '.' + tableName;
    }

}
