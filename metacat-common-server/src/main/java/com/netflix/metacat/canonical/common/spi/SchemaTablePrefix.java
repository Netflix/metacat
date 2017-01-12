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


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * shema table prefix.
 */
@ToString
@EqualsAndHashCode
@Getter
@SuppressWarnings("checkstyle:javadocmethod")
public class SchemaTablePrefix {
    /* nullable */
    private final String schemaName;
    /* nullable */
    private final String tableName;

    public SchemaTablePrefix() {
        this.schemaName = null;
        this.tableName = null;
    }

    public SchemaTablePrefix(final String schemaName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(schemaName), "schemaName is empty");
        this.schemaName = schemaName;
        this.tableName = null;
    }

    public SchemaTablePrefix(final String schemaName, final String tableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(schemaName), "schemaName is empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName is empty");
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

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
