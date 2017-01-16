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


import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

/**
 * schema table prefix.
 * @author zhenl
 */
@ToString
@EqualsAndHashCode
@NoArgsConstructor(force = true)
@Getter
public class SchemaTablePrefix {
    /* nullable */
    private final String schemaName;
    /* nullable */
    private final String tableName;

    /**
     * Constructor.
     * @param schemaName schemaName
     */
    public SchemaTablePrefix(@NonNull final String schemaName) {
        this.schemaName = schemaName;
        this.tableName = null;
    }

    /**
     * Constructor.
     * @param schemaName schemaName
     * @param tableName tableName
     */
    public SchemaTablePrefix(@NonNull final String schemaName, @NonNull final String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    /**
     * Constructor.
     * @param schemaTableName schemaTableName
     * @return boolean
     */
    public boolean matches(final SchemaTableName schemaTableName) {
        // null schema name matches everything
        if (schemaName == null) {
            return true;
        }
        if (!schemaName.equals(schemaTableName.getSchemaName())) {
            return false;
        }
        return tableName == null || tableName.equals(schemaTableName.getTableName());
    }

}
