/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.metacat.common.server.api.v1;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.DatabaseCreateRequestDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.TableDto;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Interface for methods needed by Thrift.
 * <p>
 * TODO: Get rid of this after thrift classes move to depending on services as they should
 *
 * @author tgianos
 * @since 1.1.0
 */
public interface MetacatV1 {

    /**
     * Get the table.
     *
     * @param catalogName               catalog name
     * @param databaseName              database name
     * @param tableName                 table name.
     * @param includeInfo               true if the details need to be included
     * @param includeDefinitionMetadata true if the definition metadata to be included
     * @param includeDataMetadata       true if the data metadata to be included
     * @return table
     */
    default TableDto getTable(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final boolean includeInfo,
        final boolean includeDefinitionMetadata,
        final boolean includeDataMetadata
    ) {
        return getTable(catalogName, databaseName, tableName, includeInfo, includeDefinitionMetadata,
            includeDataMetadata, false);
    }

    /**
     * Get the table.
     *
     * @param catalogName               catalog name
     * @param databaseName              database name
     * @param tableName                 table name.
     * @param includeInfo               true if the details need to be included
     * @param includeDefinitionMetadata true if the definition metadata to be included
     * @param includeDataMetadata       true if the data metadata to be included
     * @param includeInfoDetails        true if the more info details to be included
     * @return table
     */
    TableDto getTable(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final boolean includeInfo,
        final boolean includeDefinitionMetadata,
        final boolean includeDataMetadata,
        final boolean includeInfoDetails
    );

    /**
     * Returns a filtered list of table names.
     * @param catalogName  catalog name
     * @param filter       filter expression
     * @param limit        list size
     * @return list of table names
     */
    List<QualifiedName> getTableNames(
        final String catalogName,
        final String filter,
        final Integer limit
    );

    /**
     * Returns a filtered list of table names.
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param filter       filter expression
     * @param limit        list size
     * @return list of table names
     */
    List<QualifiedName> getTableNames(
        final String catalogName,
        final String databaseName,
        final String filter,
        final Integer limit
    );

    /**
     * Rename table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param newTableName new table name
     */
    void renameTable(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String newTableName
    );

    /**
     * Update table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param table        table
     * @return table
     */
    TableDto updateTable(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final TableDto table
    );

    /**
     * Creates the given database in the given catalog.
     *
     * @param catalogName              catalog name
     * @param databaseName             database name
     * @param databaseCreateRequestDto database create request
     */
    void createDatabase(
        final String catalogName,
        final String databaseName,
        @Nullable final DatabaseCreateRequestDto databaseCreateRequestDto
    );

    /**
     * Updates the given database in the given catalog.
     *
     * @param catalogName              catalog name
     * @param databaseName             database name
     * @param databaseUpdateRequestDto database update request
     */
    void updateDatabase(
        final String catalogName,
        final String databaseName,
        final DatabaseCreateRequestDto databaseUpdateRequestDto
    );

    /**
     * Creates a table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param table        TableDto with table details
     * @return created <code>TableDto</code> table
     */
    TableDto createTable(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final TableDto table
    );

    /**
     * Delete table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return deleted <code>TableDto</code> table.
     */
    TableDto deleteTable(final String catalogName, final String databaseName, final String tableName);

    /**
     * Delete database.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     */
    void deleteDatabase(final String catalogName, final String databaseName);

    /**
     * Get the database with the list of table names under it.
     *
     * @param catalogName         catalog name
     * @param databaseName        database name
     * @param includeUserMetadata true if details should include user metadata
     * @param includeTableNames   if true, then table names are listed
     * @return database with details
     */
    DatabaseDto getDatabase(final String catalogName, final String databaseName, final boolean includeUserMetadata,
        final boolean includeTableNames);

    /**
     * Get the catalog by name.
     *
     * @param catalogName catalog name
     * @return catalog
     */
    CatalogDto getCatalog(final String catalogName);

    /**
     * Get the catalog by name.
     *
     * @param catalogName catalog name
     * @param includeDatabaseNames if true, the response includes the database names
     * @param includeUserMetadata if true, the response includes the user metadata
     * @return catalog
     */
    CatalogDto getCatalog(final String catalogName, final boolean includeDatabaseNames,
                          final boolean includeUserMetadata);
}
